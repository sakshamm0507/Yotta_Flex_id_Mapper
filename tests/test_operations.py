"""Operational behavior: overrides, reference refresh, restart, backlog lifecycle, rule config."""

from __future__ import annotations

import pytest

from conftest import CANONICAL, row
from idmap.config import Rule
from idmap.ingest import ReferenceDataError
from idmap.pipeline import process_file
from idmap import db


def test_manual_override_beats_every_rule(ws):
    ws.write_overrides([("crm", "C1", "can_000004"), ("crm", "C2", "can_000005")])
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0001", email="hello@initech.example"),   # P0 wins over P1
        row("C2", tax_id="TX-100-0002"),                                  # override to an inactive canonical
        row("C3", name=" "),                                              # override not needed attributes
    ]))
    assert ws.current("C1") == [("can_000004", "manual_override")]
    assert ws.current("C2") == [("can_000002", "exact_tax_id")]   # inactive override is not a candidate
    assert ws.scalar("SELECT matched_value FROM match_candidate WHERE rule_id = 'manual_override'") == "crm:C1"


def test_override_to_unknown_canonical_is_rejected(ws):
    ws.write_overrides([("crm", "C1", "can_404404")])
    with pytest.raises(ReferenceDataError, match="unknown canonical_id"):
        ws.run(ws.batch("crm_batch_001.csv", [row("C1")]))


def test_duplicate_canonical_id_is_rejected(ws):
    ws.write_canonical(CANONICAL + [CANONICAL[0]])
    with pytest.raises(ReferenceDataError, match="duplicate canonical_id"):
        ws.run(ws.batch("crm_batch_001.csv", [row("C1")]))


def test_reference_reload_rebuilds_keys(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", email="new@acme.example")]))
    assert ws.status("C1")[0] == "UNRESOLVED"
    changed = [(*CANONICAL[0][:3], "new@acme.example", *CANONICAL[0][4:])] + CANONICAL[1:]
    ws.write_canonical(changed)
    ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts="2026-09-02T00:00:00Z", email="new@acme.example")]))
    assert ws.current("C1") == [("can_000001", "exact_email")]
    assert ws.query("SELECT status, attempt_count FROM unresolved_mapping") == [("RESOLVED", 1)]


def test_unresolved_backlog_lifecycle(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", ts="2026-09-01T00:00:00Z", name="Nobody")]))
    ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts="2026-09-02T00:00:00Z", name="Nobody Else")]))
    assert ws.query("SELECT status, attempt_count FROM unresolved_mapping") == [("OPEN", 2)]
    third = ws.run_one(ws.batch("crm_batch_003.csv", [row("C1", ts="2026-09-03T00:00:00Z", tax_id="TX-100-0002")]))
    assert ws.query("SELECT status, resolved_run_id, attempt_count FROM unresolved_mapping") == [
        ("RESOLVED", third.run_id, 2)]
    ws.assert_checks_pass()


def test_unresolved_with_existing_mapping_keeps_it(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", ts="2026-09-01T00:00:00Z", tax_id="TX-100-0001")]))
    ws.run_one(ws.batch("crm_batch_002.csv", [row("C1", ts="2026-09-02T00:00:00Z", name="Renamed Co")]))
    assert ws.current("C1") == [("can_000001", "exact_tax_id")]
    assert ws.status("C1") == ("UNRESOLVED", "NO_CANDIDATE", "can_000001")
    assert ws.query("SELECT has_existing_mapping FROM unresolved_mapping") == [(True,)]
    ws.assert_checks_pass()


def test_abandoned_running_run_is_marked_failed(ws):
    con = db.connect(ws.cfg)
    con.execute("""INSERT INTO pipeline_run (run_id, run_seq, source_file, status, started_at)
                   VALUES ('run_dead', 1, 'x.csv', 'RUNNING', TIMESTAMP '2026-09-01 00:00:00')""")
    con.close()
    ws.run(ws.batch("crm_batch_001.csv", [row("C1", tax_id="TX-100-0001")]))
    assert ws.query("SELECT status, failure_reason FROM pipeline_run WHERE run_id = 'run_dead'") == [
        ("FAILED", "Abandoned: process exited before the run finished")]


def test_restart_processes_only_unfinished_files_in_name_order(ws):
    ws.batch("b_batch.csv", [row("C2", tax_id="TX-100-0002")])
    ws.batch("a_batch.csv", [row("C1", tax_id="TX-100-0001")])
    first = ws.run()
    assert [r.source_file.rsplit("/", 1)[-1] for r in first] == ["a_batch.csv", "b_batch.csv"]
    ws.batch("c_batch.csv", [row("C3", tax_id="TX-100-0003")])
    second = ws.run()
    assert [r.status for r in second] == ["SKIPPED", "SKIPPED", "SUCCEEDED"]


def test_backfill_older_file_after_newer(ws):
    ws.run_one(ws.batch("2026_10.csv", [row("C1", ts="2026-10-01T00:00:00Z", tax_id="TX-100-0002")]))
    ws.run_one(ws.batch("2026_08.csv", [row("C1", ts="2026-08-01T00:00:00Z", tax_id="TX-100-0001"),
                                        row("C7", ts="2026-08-01T00:00:00Z", tax_id="TX-100-0007")]))
    assert ws.current("C1") == [("can_000002", "exact_tax_id")]   # protected from stale backfill
    assert ws.current("C7") == [("can_000007", "exact_tax_id")]   # new keys from the backfill still load
    assert ws.scalar("SELECT count(*) FROM late_arrival_log") == 1


def test_disabled_rule_and_shared_priority(ws):
    rules = (Rule("manual_override", 0), Rule("exact_tax_id", 1), Rule("exact_email", 1),
             Rule("rule_name_domain", 3, enabled=False), Rule("rule_phone_name", 4))
    ws.cfg = ws.cfg.replace(rules=rules)
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", tax_id="TX-100-0001", email="info@globex.example"),   # P1 and P2 tie -> conflict
        row("C2", name="Stark Industries", domain="stark.example"),      # name+domain disabled
    ]))
    assert ws.query("SELECT rule_id, candidate_canonical_ids FROM mapping_conflict") == [
        ("exact_email+exact_tax_id", ["can_000001", "can_000002"])]
    assert ws.status("C2") == ("UNRESOLVED", "NO_CANDIDATE", None)


def test_missing_file_is_recorded_as_failed(ws):
    con = db.connect(ws.cfg)
    try:
        result = process_file(con, ws.cfg, ws.incoming / "missing.csv")
    finally:
        con.close()
    assert result.status == "FAILED" and "batch file not found" in result.failure_reason
    assert ws.scalar("SELECT failed_stage FROM pipeline_run") == "ingest"
