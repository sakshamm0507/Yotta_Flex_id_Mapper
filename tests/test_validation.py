"""Row validation edge cases beyond the plan's core list."""

from __future__ import annotations

from datetime import datetime

from conftest import HEADER, row


def test_reason_precedence_first_failing_rule_wins(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("", email="bad@", phone="12"),                     # missing id beats bad email/phone
        row("E1", system="erp", ts="not-a-date"),              # unknown system beats bad timestamp
        row("C3", ts="epoch", email="bad@"),                   # bad timestamp beats bad email
        row("C4", email="bad@", phone="12"),                   # email checked before phone
        row("C5", phone="12", canonical_ref="can_404"),        # phone checked before canonical_ref
    ]))
    assert ws.quarantine() == [(2, "MISSING_REQUIRED_FIELD"), (3, "UNKNOWN_SOURCE_SYSTEM"), (4, "TYPE_CAST_FAILED"),
                               (5, "INVALID_EMAIL"), (6, "INVALID_PHONE")]


def test_type_cast_and_timezone(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [
        row("C1", ts="2026-09-01T15:30:00+05:30", tax_id="TX-100-0001"),
        row("C2", ts="2026-02-30T00:00:00Z", tax_id="TX-100-0002"),
        row("C3", ts="31/12/2025 10:00", tax_id="TX-100-0003"),
    ]))
    assert ws.scalar("SELECT source_updated_at FROM stg_source_record WHERE source_id = 'C1'") == datetime(2026, 9, 1, 10, 0)
    assert ws.quarantine() == [(3, "TYPE_CAST_FAILED"), (4, "TYPE_CAST_FAILED")]


def test_source_system_is_case_insensitive(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", system=" CRM ", tax_id="TX-100-0001")]))
    assert ws.current("C1", "crm") == [("can_000001", "exact_tax_id")]


def test_reject_rate_circuit_breaker(ws):
    rows = [row(f"C{i}", email="broken@") for i in range(60)] + [row(f"D{i}", tax_id="TX-100-0001") for i in range(40)]
    result = ws.run_one(ws.batch("crm_batch_001.csv", rows))
    assert result.status == "FAILED"
    assert "RejectRateExceeded" in result.failure_reason and "60/100" in result.failure_reason
    assert all(n == 0 for n in ws.table_counts().values())


def test_reject_rate_ignored_for_small_files(ws):
    result = ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", email="broken@"), row("C2", email="broken@")]))
    assert result.status == "SUCCEEDED" and result.counts["rows_quarantined"] == 2


def test_header_only_whitespace_and_bom(ws):
    text = "﻿" + ",".join(h.upper() for h in HEADER) + "\n\ncrm,C1,,,,,TX-100-0001,,2026-09-01T00:00:00Z\n"
    result = ws.run_one(ws.raw("crm_batch_001.csv", text))
    assert result.status == "SUCCEEDED" and result.counts["rows_read"] == 1   # blank line is not a record
    assert ws.query("SELECT source_row_num FROM stg_source_record") == [(3,)]


def test_zero_byte_file_fails_schema(ws):
    result = ws.run_one(ws.raw("crm_batch_001.csv", ""))
    assert result.status == "FAILED" and "no header row" in result.failure_reason


def test_duplicate_header_fails(ws):
    result = ws.run_one(ws.raw("crm_batch_001.csv", ",".join(HEADER + ["email"]) + "\n"))
    assert result.status == "FAILED" and "SCHEMA_DUPLICATE_COLUMN" in result.failure_reason


def test_raw_payload_keeps_original_text(ws):
    ws.run_one(ws.batch("crm_batch_001.csv", [row("C1", name="  Acme  ", email="Broken@ ")]))
    payload = ws.scalar("SELECT raw_payload FROM quarantine_record")
    assert '"name":"  Acme  "' in payload and '"email":"Broken@ "' in payload
