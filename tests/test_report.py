"""Reconciliation checks: they pass on correct state and catch tampered state."""

from __future__ import annotations

import csv

import pytest

from conftest import row
from idmap import db, report

T1, T2 = "2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z"


@pytest.fixture
def loaded(ws):
    ws.run(
        ws.batch("crm_batch_001.csv", [
            row("C1", ts=T1, tax_id="TX-100-0001"), row("C2", email="hello@initech.example"),
            row("C3", name="Nobody"), row("C4", email="bad@"), row("C5", ts=T2, tax_id="TX-100-0006"),
            row("C5", ts=T2, tax_id="TX-100-0006"),
        ]),
        ws.batch("crm_batch_002.csv", [row("C1", ts=T2, tax_id="TX-100-0002"), row("C5", ts=T1, tax_id="TX-100-0007")]),
    )
    return ws


def _tamper(ws, sql: str) -> None:
    con = db.connect(ws.cfg)
    con.execute(sql)
    con.close()


def test_all_checks_pass_and_determinism_replays(loaded):
    checks = loaded.checks(determinism=True)
    assert {k: c.status for k, c in checks.items()} == {k: "PASS" for k in checks}
    assert "replayed 2 run(s)" in checks["7"].detail


@pytest.mark.parametrize("sql, check", [
    ("UPDATE pipeline_run SET rows_read = rows_read + 1 WHERE run_seq = 1", "1"),
    ("UPDATE pipeline_run SET mapped = mapped + 1 WHERE run_seq = 1", "1b"),
    ("DELETE FROM stg_source_record WHERE source_id = 'C3'", "1c"),
    ("DELETE FROM source_key_status WHERE source_id = 'C3'", "2"),
    ("UPDATE id_mapping_history SET is_current = true, valid_to = NULL, closed_run_id = NULL", "3"),
    ("UPDATE canonical_entity SET is_active = false WHERE canonical_id = 'can_000002'", "4"),
    ("UPDATE id_mapping_history SET valid_to = TIMESTAMP '2026-09-15' WHERE NOT is_current", "5"),
    ("UPDATE quarantine_record SET reject_reason_code = 'SOMETHING_ELSE'", "6"),
    ("UPDATE source_key_status SET canonical_id = 'can_000007' WHERE source_id = 'C1'", "8"),
    ("UPDATE mapping_conflict SET status = 'RESOLVED' WHERE conflict_type = 'ONE_TO_MANY'", "9"),
    ("INSERT INTO match_candidate SELECT * FROM match_candidate LIMIT 1", "10"),
])
def test_checks_detect_violations(loaded, sql, check):
    _tamper(loaded, sql)
    assert loaded.checks(determinism=False)[check].status == "FAIL"


def test_determinism_detects_divergence(loaded):
    _tamper(loaded, "UPDATE id_mapping_history SET match_rule = 'exact_email' WHERE is_current")
    result = loaded.checks(determinism=True)["7"]
    assert result.status == "FAIL" and "id_mapping_history" in result.detail


def test_determinism_skips_when_input_changed(loaded):
    (loaded.incoming / "crm_batch_002.csv").write_text("changed\n")
    assert loaded.checks(determinism=True)["7"].status == "SKIP"


def test_quality_metrics_against_labels(loaded, tmp_path):
    expected = tmp_path / "expected"
    expected.mkdir()
    with (expected / "expected_labels.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["source_system", "source_id", "expected_status", "expected_canonical_id", "expected_rule",
                    "true_canonical_id", "scenario"])
        w.writerows([
            ["crm", "C1", "MAPPED", "can_000002", "exact_tax_id", "can_000002", "remap"],
            ["crm", "C2", "CONFLICT", "", "exact_email", "can_000003", "one_to_many"],
            ["crm", "C3", "UNRESOLVED", "", "", "", "no_match"],
            ["crm", "C4", "QUARANTINED", "", "", "can_000006", "bad_email"],
            ["crm", "C5", "MAPPED", "can_000006", "exact_tax_id", "can_000007", "wrong_on_purpose"],
        ])
    with (expected / "expected_quarantine.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["source_file", "source_row_num", "expected_reason_code"])
        w.writerow(["crm_batch_001.csv", 5, "INVALID_EMAIL"])
    con = db.connect(loaded.cfg)
    try:
        q = report.quality_metrics(con, expected)
    finally:
        con.close()
    assert q["spec_agreement"] == (5, 5)
    assert q["precision_counts"] == (1, 2)       # C1 right, C5 mapped to the wrong entity
    assert q["recall_counts"] == (1, 3)          # C1, C2 and C5 have active true entities
    assert q["quarantine_expected"] == [("INVALID_EMAIL", 1, 1)]
    assert q["history"][3] == 1                  # one late arrival (C5)


def test_build_report_and_export(loaded, tmp_path):
    ok, checks = report.build_report(loaded.cfg, tmp_path / "report.md", None)
    text = (tmp_path / "report.md").read_text()
    assert ok and "## Verdict: **PASS**" in text and "No `expected_labels.csv` found" in text
    written = report.export_tables(loaded.cfg, tmp_path / "tables")
    assert {p.name for p in written} >= {"id_mapping_history.csv", "pipeline_run.csv", "quarantine_record.csv"}
    with (tmp_path / "tables" / "id_mapping_history.csv").open() as fh:
        assert len(list(csv.DictReader(fh))) == 3

