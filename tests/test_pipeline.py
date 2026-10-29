"""The 22 scenario tests from the project plan (section 10), in plan order.

Each test builds a small batch by hand, so the expected outcome can be read off the fixture.
"""

from __future__ import annotations

import unicodedata
from datetime import datetime
from pathlib import Path

import pytest

from conftest import HEADER, Workspace, row
from idmap.pipeline import STAGES

T1, T2, T3 = "2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z", "2026-09-20T00:00:00Z"


# 1
def test_happy_path(ws):
    batch = ws.batch("crm_batch_001.csv", [
        row("C1", name="ACME WIDGETS", tax_id="tx 100-0001"),
        row("C2", name="Globex", email=" INFO@globex.example "),
        row("C3", name="STARK INDUSTRIES", domain="https://www.stark.example/"),
        row("C4", name="Wayne Ent", phone="(415) 555-0107"),
    ])
    result = ws.run_one(batch)
    assert result.status == "SUCCEEDED"
    c = result.counts
    assert (c["rows_read"], c["rows_staged"], c["rows_quarantined"], c["duplicates_dropped"]) == (4, 4, 0, 0)
    assert (c["mapped"], c["conflicts"], c["unresolved"], c["mappings_inserted"]) == (4, 0, 0, 4)
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    assert ws.current("C2") == [("can_000002", "exact_email")]
    assert ws.current("C3") == [("can_000006", "rule_name_domain")]
    assert ws.current("C4") == [("can_000007", "rule_phone_name")]
    ws.assert_checks_pass()


# 2
def test_empty_file(ws):
    result = ws.run_one(ws.batch("crm_batch_001.csv", []))
    assert result.status == "SUCCEEDED"
    assert result.counts["rows_read"] == 0 and result.counts["rows_staged"] == 0
    assert result.failure_reason is None
    assert all(n == 0 for n in ws.table_counts().values())
    ws.assert_checks_pass()


# 3
def test_malformed_rows(ws):
    text = (",".join(HEADER) + "\n"
            "crm,C1,Acme Widgets,,,,TX1000001,,2026-09-01T00:00:00Z\n"        # line 2
            "crm,C2,Acme, Inc,,,,,,2026-09-01T00:00:00Z\n"                    # line 3: unquoted comma
            "crm,C3,Globex\n"                                                 # line 4: truncated
            'crm,C4,"Multi\nline Globex",info@globex.example,,,,,2026-09-01T00:00:00Z\n'  # lines 5-6
            "crm,C5,Bad \xff bytes,,,,,,2026-09-01T00:00:00Z\n"               # line 7: invalid UTF-8
            "crm,C6,Stark,,,,TX-100-0006,,2026-09-01T00:00:00Z\n")            # line 8
    path = ws.raw("crm_batch_001.csv", text.encode("utf-8").replace(b"\xc3\xbf", b"\xff"))
    result = ws.run_one(path)
    assert result.status == "SUCCEEDED"
    assert ws.quarantine() == [(3, "MALFORMED_ROW"), (4, "MALFORMED_ROW"), (7, "MALFORMED_ROW")]
    details = dict(ws.query("SELECT source_row_num, reject_detail FROM quarantine_record"))
    assert details[3] == "expected 9 columns, found 10" and "UTF-8" in details[7]
    staged = ws.query("SELECT source_id, source_row_num FROM stg_source_record ORDER BY source_id")
    assert staged == [("C1", 2), ("C4", 5), ("C6", 8)]
    assert ws.current("C4") == [("can_000002", "exact_email")]
    assert result.counts["rows_read"] == 6


# 4
def test_missing_required_field(ws):
    path = ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0001"),                     # line 2: valid
        row("", tax_id="TX-100-0002"),                       # line 3: no source_id
        row("C3", ts="", tax_id="TX-100-0003"),              # line 4: no source_updated_at
        row("C4", system="  ", tax_id="TX-100-0004"),        # line 5: whitespace-only source_system
    ])
    ws.run_one(path)
    rows = ws.query("SELECT source_row_num, reject_reason_code, reject_detail, run_id, source_file "
                    "FROM quarantine_record ORDER BY source_row_num")
    assert [(r[0], r[1]) for r in rows] == [(3, "MISSING_REQUIRED_FIELD"), (4, "MISSING_REQUIRED_FIELD"),
                                            (5, "MISSING_REQUIRED_FIELD")]
    assert [r[2] for r in rows] == ["null: source_id", "null: source_updated_at", "null: source_system"]
    assert all(r[3] and r[4].endswith("crm_batch_001.csv") for r in rows)
    payload = ws.scalar("SELECT raw_payload->>'tax_id' FROM quarantine_record WHERE source_row_num = 3")
    assert payload == "TX-100-0002"


# 5
def test_nulls_in_optional_fields(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", name="Stark Industries", domain="stark.example"),   # no tax, email, phone
        row("C2", name="Globex Corp.", phone="415.555.0102"),          # only name + phone
    ]))
    assert ws.current("C1") == [("can_000006", "rule_name_domain")]
    assert ws.current("C2") == [("can_000002", "rule_phone_name")]


# 6
def test_insufficient_attributes(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", phone="+1 415 555 0101", domain="acme.example"),
        row("C2", name="   ", email="\t", tax_id="   ", domain="acme.example"),  # whitespace-only = null
        row("C3", name="Unknown Org"),                                              # name present, no hit
    ]))
    assert ws.status("C1") == ("UNRESOLVED", "INSUFFICIENT_ATTRIBUTES", None)
    assert ws.status("C2") == ("UNRESOLVED", "INSUFFICIENT_ATTRIBUTES", None)
    assert ws.status("C3") == ("UNRESOLVED", "NO_CANDIDATE", None)
    assert ws.scalar("SELECT count(*) FROM unresolved_mapping WHERE status = 'OPEN'") == 3
    assert ws.scalar("SELECT count(*) FROM quarantine_record") == 0


# 7
def test_duplicate_exact(ws):
    result = ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", name="Acme", tax_id="TX-100-0001"),
        row("C1", name="Acme", tax_id="TX-100-0001"),
        row("C1", name="  Acme ", tax_id="TX-100-0001 "),     # identical after trimming
    ]))
    c = result.counts
    assert (c["rows_read"], c["rows_staged"], c["duplicates_dropped"], c["rows_quarantined"]) == (3, 1, 2, 0)
    assert ws.query("SELECT source_row_num FROM stg_source_record") == [(2,)]
    ws.assert_checks_pass()


# 8
def test_duplicate_key_conflicting_payload(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", ts="2026-09-02T00:00:00Z", tax_id="TX-100-0001"),   # line 2: newest -> kept
        row("C1", ts="2026-09-01T00:00:00Z", tax_id="TX-100-0002"),   # line 3: older -> superseded
        row("C2", email="info@globex.example"),                       # line 4: tie, earlier line
        row("C2", email="office@stark.example"),                      # line 5: tie, later line wins
    ]))
    rows = ws.query("SELECT source_row_num, reject_reason_code, reject_detail FROM quarantine_record ORDER BY 1")
    assert [(r[0], r[1]) for r in rows] == [(3, "DUPLICATE_KEY_SUPERSEDED"), (4, "DUPLICATE_KEY_SUPERSEDED")]
    assert "superseded by row 2" in rows[0][2] and "same source_updated_at" in rows[1][2]
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    assert ws.current("C2") == [("can_000006", "exact_email")]


# 9
def test_schema_extra_column(ws):
    header = ["source_id", "loyalty_tier", "source_system", "name", "email", "phone", "domain", "tax_id", "source_updated_at"]
    path = ws.batch("crm_batch_001.csv", [{**row("C1", tax_id="TX-100-0001"), "loyalty_tier": "gold"}], header=header)
    result = ws.run_one(path)
    assert result.status == "SUCCEEDED"
    assert result.counts["schema_warnings"] == 2   # extra loyalty_tier + optional canonical_ref absent
    assert ws.scalar("SELECT schema_warnings FROM pipeline_run") == 2
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    columns = [r[0] for r in ws.query("SELECT column_name FROM duckdb_columns() WHERE table_name = 'stg_source_record'")]
    assert "loyalty_tier" not in columns
    events = [line for line in result.log_file.read_text().splitlines() if "schema warning" in line]
    assert len(events) == 2 and "loyalty_tier" in events[0]


# 10
def test_schema_missing_column(ws):
    header = [c for c in HEADER if c != "source_updated_at"]
    result = ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", tax_id="TX-100-0001")], header=header))
    assert result.status == "FAILED"
    assert "SCHEMA_MISSING_COLUMN" in result.failure_reason
    run = ws.query("SELECT status, failed_stage, failure_reason FROM pipeline_run")[0]
    assert run[0] == "FAILED" and run[1] == "schema" and "source_updated_at" in run[2]
    assert all(n == 0 for n in ws.table_counts().values())
    assert ws.query("SELECT status FROM processed_file") == [("FAILED",)]


# 11
def test_invalid_canonical_ref(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0001", canonical_ref="can_999999"),
        row("C2", tax_id="TX-100-0002", canonical_ref="can_000002"),
    ]))
    assert ws.quarantine() == [(2, "INVALID_CANONICAL_REF")]
    assert ws.scalar("SELECT reject_detail FROM quarantine_record") == "canonical_ref=can_999999"
    assert ws.current("C2") == [("can_000002", "exact_tax_id")]


# 12
def test_rerun_same_file_skipped(ws):
    path = ws.batch("crm_batch_001.csv", [row("C1", tax_id="TX-100-0001"), row("C2", email="bad@")])
    first = ws.run_one(path)
    before = ws.table_counts()
    second = ws.run_one(path)
    assert second.status == "SKIPPED"
    assert first.run_id in second.status_detail
    assert ws.table_counts() == before
    assert ws.query("SELECT status, count(*) FROM pipeline_run GROUP BY 1 ORDER BY 1") == [("SKIPPED", 1), ("SUCCEEDED", 1)]
    # Same content under another name is still the same batch.
    copy = ws.raw("crm_batch_001_copy.csv", path.read_bytes())
    assert ws.run_one(copy).status == "SKIPPED"


def _mixed_batch(ws: Workspace) -> Path:
    return ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0001"),                       # mapped
        row("C2", email="hello@initech.example"),              # conflict
        row("C3", name="Nobody Inc"),                          # unresolved
        row("C4", email="broken@"),                            # quarantined
        row("C5", tax_id="TX-100-0006"), row("C5", tax_id="TX-100-0006"),  # exact duplicate
        row("C6", tax_id="TX-100-0001"),                       # many-to-one with C1
    ])


# 13
def test_forced_rerun_no_duplicates(ws):
    path = _mixed_batch(ws)
    ws.run_one(path)
    before_counts, before_fp = ws.table_counts(), ws.fingerprints()
    mapping_ids = ws.query("SELECT mapping_id FROM id_mapping_history ORDER BY 1")
    forced = ws.run_one(path, force=True)
    assert forced.status == "SUCCEEDED"
    assert ws.table_counts() == before_counts
    assert ws.fingerprints() == before_fp
    assert ws.query("SELECT mapping_id FROM id_mapping_history ORDER BY 1") == mapping_ids
    assert ws.scalar("SELECT attempt_count FROM unresolved_mapping WHERE source_id = 'C3'") == 1
    assert forced.counts["mappings_inserted"] == 0
    ws.assert_checks_pass()


# 14
@pytest.mark.parametrize("stage", STAGES)
def test_partial_failure_then_retry(ws, tmp_path, monkeypatch, stage):
    path = _mixed_batch(ws)
    monkeypatch.setenv("IDMAP_FAIL_AT_STAGE", stage)
    failed = ws.run_one(path)
    assert failed.status == "FAILED"
    assert f"injected failure at stage '{stage}'" in failed.failure_reason
    assert ws.scalar("SELECT failed_stage FROM pipeline_run") == stage
    assert all(n == 0 for n in ws.table_counts().values()), "rollback must leave no partial writes"
    monkeypatch.delenv("IDMAP_FAIL_AT_STAGE")

    retried = ws.run_one(path)   # not SUCCEEDED before, so it is picked up again without --force
    assert retried.status == "SUCCEEDED"
    (tmp_path / "clean").mkdir()
    clean = Workspace(tmp_path / "clean")
    clean.run_one(clean.raw("crm_batch_001.csv", path.read_bytes()))
    state = ("id_mapping_history", "source_key_status", "mapping_conflict", "unresolved_mapping", "match_candidate")
    got, want = ws.fingerprints(), clean.fingerprints()
    assert {t: got[t] for t in state} == {t: want[t] for t in state}
    assert ws.table_counts() == clean.table_counts()
    ws.assert_checks_pass()


# 15
def test_one_to_many_conflict(ws):
    result = ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", email="hello@initech.example"),
        row("C2", name="Initech", domain="initech.example"),
    ]))
    assert result.counts["conflicts"] == 2
    assert ws.current("C1") == [] and ws.current("C2") == []
    conflicts = ws.query("""SELECT source_id, conflict_type, candidate_canonical_ids, rule_id, status,
                                   has_existing_mapping FROM mapping_conflict ORDER BY source_id""")
    assert conflicts == [
        ("C1", "ONE_TO_MANY", ["can_000003", "can_000004"], "exact_email", "OPEN", False),
        ("C2", "ONE_TO_MANY", ["can_000003", "can_000004"], "rule_name_domain", "OPEN", False),
    ]
    assert ws.status("C1") == ("CONFLICT", "ONE_TO_MANY", None)
    assert ws.scalar("SELECT count(*) FROM match_candidate WHERE source_id = 'C1'") == 2


# 16
def test_priority_precedence(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        # P1 tax -> can_1 beats P3 name+domain -> can_6
        row("C1", tax_id="TX-100-0001", name="Stark Industries", domain="stark.example"),
        # P2 email -> can_2 beats P4 phone+name -> can_7
        row("C2", email="info@globex.example", name="Wayne", phone="+1 415 555 0107"),
        # P1 decides even though P2 alone would be a conflict
        row("C3", tax_id="TX-100-0003", email="hello@initech.example"),
    ]))
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    assert ws.current("C2") == [("can_000002", "exact_email")]
    assert ws.current("C3") == [("can_000003", "exact_tax_id")]
    assert ws.scalar("SELECT count(*) FROM mapping_conflict") == 0
    hits = ws.query("SELECT rule_id, canonical_id FROM match_candidate WHERE source_id = 'C1' ORDER BY rule_priority")
    assert hits == [("exact_tax_id", "can_000001"), ("rule_name_domain", "can_000006")]


# 17
def test_remap_closes_history(ws):
    first = ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", ts=T1, tax_id="TX-100-0001")]))
    second = ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts=T2, tax_id="TX-100-0002")]))
    assert second.counts["mappings_closed"] == 1 and second.counts["mappings_inserted"] == 1
    history = ws.query("""SELECT canonical_id, valid_from, valid_to, is_current, created_run_id, closed_run_id
                          FROM id_mapping_history ORDER BY valid_from""")
    assert history == [
        ("can_000001", datetime(2026, 9, 1), datetime(2026, 9, 10), False, first.run_id, second.run_id),
        ("can_000002", datetime(2026, 9, 10), None, True, second.run_id, None),
    ]
    assert ws.scalar("SELECT count(*) FROM id_mapping_current WHERE source_id = 'C1'") == 1
    assert ws.status("C1") == ("MAPPED", "exact_tax_id", "can_000002")
    ws.assert_checks_pass()


# 18
def test_late_arrival_does_not_override(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", ts=T2, tax_id="TX-100-0001")]))
    late = ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts=T1, tax_id="TX-100-0002")]))
    assert late.counts["late_arrivals"] == 1 and late.counts["mappings_inserted"] == 0
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    log = ws.query("""SELECT record_updated_at, latest_applied_updated_at, current_valid_from, current_canonical_id,
                             proposed_status, proposed_canonical_id, action FROM late_arrival_log""")
    assert log == [(datetime(2026, 9, 1), datetime(2026, 9, 10), datetime(2026, 9, 10), "can_000001",
                    "MAPPED", "can_000002", "IGNORED_FOR_CURRENT")]
    assert ws.scalar("SELECT last_source_updated_at FROM source_key_status") == datetime(2026, 9, 10)

    # Newer than valid_from but older than the latest evidence is still late:
    # t1 maps A, t3 confirms A, then t2 says B -> B is ignored.
    ws.run_one(ws.batch("crm_batch_003.csv", [row("C9", ts=T1, tax_id="TX-100-0001")]))
    ws.run_one(ws.batch("crm_batch_004.csv", [row("C9", ts=T3, tax_id="TX-100-0001")]))
    ws.run_one(ws.batch("crm_batch_005.csv", [row("C9", ts=T2, tax_id="TX-100-0002")]))
    assert ws.current("C9") == [("can_000001", "exact_tax_id")]
    assert ws.scalar("SELECT count(*) FROM late_arrival_log WHERE source_id = 'C9'") == 1
    ws.assert_checks_pass()


# 19
def test_normalization_collision(ws):
    nfd_name = unicodedata.normalize("NFD", "Société Générale des Eaux")
    ws.run_one(ws.batch("mixed_batch_001.csv", [
        row("C1", email=" CONTACT@ACME.EXAMPLE "),
        row("B1", system="billing", email="contact@acme.example"),
        row("S1", system="support", tax_id="tx 100 0001"),
        row("C2", name="SOCIÉTÉ GÉNÉRALE DES EAUX", domain="http://www.SGE.example/contact"),
        row("B2", system="billing", name=nfd_name, domain="sge.example"),
        row("S2", system="support", name="Société  Générale-des Eaux", phone="+44 (20) 7946-0958"),
    ]))
    for system, sid in (("crm", "C1"), ("billing", "B1"), ("support", "S1")):
        assert ws.current(sid, system)[0][0] == "can_000001"
    for system, sid in (("crm", "C2"), ("billing", "B2"), ("support", "S2")):
        assert ws.current(sid, system)[0][0] == "can_000008"
    norms = ws.query("SELECT DISTINCT email_norm FROM stg_source_record WHERE email_norm IS NOT NULL")
    assert norms == [("contact@acme.example",)]


# 20
def test_inactive_canonical_excluded(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0005"),                                  # only the inactive canonical
        row("C2", tax_id="TX-100-0005", email="info@globex.example"),     # inactive P1 skipped, P2 decides
    ]))
    assert ws.status("C1") == ("UNRESOLVED", "INACTIVE_CANONICAL_ONLY", None)
    assert ws.scalar("SELECT count(*) FROM match_candidate WHERE source_id = 'C1'") == 0
    assert ws.current("C2") == [("can_000002", "exact_email")]
    ws.assert_checks_pass()


# 21
def test_conflict_keeps_existing_mapping(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", ts=T1, tax_id="TX-100-0003")]))
    ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts=T2, email="hello@initech.example")]))
    assert ws.current("C1") == [("can_000003", "exact_tax_id")]
    assert ws.status("C1") == ("CONFLICT", "ONE_TO_MANY", "can_000003")
    conflict = ws.query("SELECT status, has_existing_mapping, existing_canonical_id FROM mapping_conflict")
    assert conflict == [("OPEN", True, "can_000003")]
    ws.assert_checks_pass()

    third = ws.run_one(ws.batch("crm_batch_003.csv", [row("C1", ts=T3, tax_id="TX-100-0003")]))
    assert ws.query("SELECT status, resolved_run_id FROM mapping_conflict") == [("RESOLVED", third.run_id)]
    assert ws.status("C1") == ("MAPPED", "exact_tax_id", "can_000003")
    assert ws.scalar("SELECT count(*) FROM id_mapping_history") == 1
    ws.assert_checks_pass()


# 22
def test_many_to_one_same_system_flagged(ws):
    ws.run_one(ws.batch("mixed_batch_001.csv", [
        row("C1", ts=T1, tax_id="TX-100-0001"),
        row("C2", ts=T1, email="contact@acme.example"),
        row("B1", system="billing", ts=T1, tax_id="TX-100-0001"),   # other system: not a warning
    ]))
    flags = ws.query("""SELECT source_system, source_id, candidate_canonical_ids, status
                        FROM mapping_conflict WHERE conflict_type = 'MANY_TO_ONE_SAME_SYSTEM' ORDER BY source_id""")
    assert flags == [("crm", "C1", ["can_000001"], "OPEN"), ("crm", "C2", ["can_000001"], "OPEN")]
    assert ws.status("C1")[0] == ws.status("C2")[0] == "MAPPED"   # warning only: mappings stay
    assert ws.scalar("SELECT many_to_one_open FROM pipeline_run") == 2

    ws.run_one(ws.batch("crm_batch_002.csv", [row("C2", ts=T2, tax_id="TX-100-0002")]))
    assert ws.query("SELECT DISTINCT status FROM mapping_conflict") == [("RESOLVED",)]
    ws.assert_checks_pass()
