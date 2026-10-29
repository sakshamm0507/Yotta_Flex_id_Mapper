"""Shared fixtures: an isolated workspace (own DuckDB file, master, logs) per test."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from idmap import db, metrics, report
from idmap.config import load_config
from idmap.pipeline import RunResult, run_pipeline

HEADER = ["source_system", "source_id", "name", "email", "phone", "domain", "tax_id", "canonical_ref", "source_updated_at"]
TS = "2026-09-01T00:00:00Z"

FIXTURES = Path(__file__).parent / "fixtures"

# Small hand-built master (tests/fixtures/canonical_entity.csv). can_000003 and can_000004 share an
# email and normalize to the same name on the same domain (designed one-to-many). can_000005 is inactive.
with (FIXTURES / "canonical_entity.csv").open(newline="", encoding="utf-8") as _fh:
    CANONICAL = [tuple(r) for r in list(csv.reader(_fh))[1:]]


def row(source_id: str, system: str = "crm", ts: str = TS, **fields: str) -> dict[str, str]:
    """A source row; unspecified attributes are empty."""
    return {"source_system": system, "source_id": source_id, "source_updated_at": ts, **fields}


class Workspace:
    def __init__(self, root: Path):
        self.root = root
        self.incoming = root / "incoming"
        self.incoming.mkdir()
        self.cfg = load_config().replace(
            db_path=root / "idmap.duckdb",
            incoming_dir=self.incoming,
            incoming_pattern="*.csv",
            log_dir=root / "logs",
            report_file=root / "report.md",
            canonical_file=root / "canonical_entity.csv",
            override_file=root / "mapping_override.csv",
            expected_dir=None,
        )
        self.write_canonical(CANONICAL)
        self.write_overrides([])

    # -- inputs ------------------------------------------------------------------------
    def write_canonical(self, rows) -> None:
        with self.cfg.canonical_file.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["canonical_id", "legal_name", "tax_id", "primary_email", "domain", "phone", "is_active", "created_at"])
            for r in rows:
                w.writerow([*r, "2025-01-01T00:00:00Z"])

    def write_overrides(self, rows) -> None:
        with self.cfg.override_file.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["source_system", "source_id", "canonical_id", "reason", "created_at"])
            for r in rows:
                w.writerow([*r, "steward", "2026-08-01T00:00:00Z"])

    def batch(self, name: str, rows: list[dict], header: list[str] | None = None) -> Path:
        header = header or HEADER
        path = self.incoming / name
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(header)
            for r in rows:
                w.writerow([r.get(c, "") for c in header])
        return path

    def raw(self, name: str, text: str | bytes) -> Path:
        path = self.incoming / name
        if isinstance(text, bytes):
            path.write_bytes(text)
        else:
            path.write_text(text, encoding="utf-8", newline="")
        return path

    # -- actions -----------------------------------------------------------------------
    def run(self, *paths: Path, force: bool = False) -> list[RunResult]:
        return run_pipeline(self.cfg, files=list(paths) if paths else None, force=force)

    def run_one(self, path: Path, force: bool = False) -> RunResult:
        (result,) = self.run(path, force=force)
        return result

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        con = db.connect(self.cfg)
        try:
            return con.execute(sql, params or []).fetchall()
        finally:
            con.close()

    def scalar(self, sql: str, params: list | None = None):
        rows = self.query(sql, params)
        return rows[0][0] if rows else None

    def status(self, source_id: str, system: str = "crm") -> tuple | None:
        rows = self.query(
            "SELECT status, status_reason, canonical_id FROM source_key_status WHERE source_system = ? AND source_id = ?",
            [system, source_id],
        )
        return rows[0] if rows else None

    def current(self, source_id: str, system: str = "crm") -> list[tuple]:
        return self.query(
            "SELECT canonical_id, match_rule FROM id_mapping_current WHERE source_system = ? AND source_id = ?",
            [system, source_id],
        )

    def quarantine(self) -> list[tuple]:
        return self.query("SELECT source_row_num, reject_reason_code FROM quarantine_record ORDER BY source_row_num")

    def table_counts(self) -> dict[str, int]:
        tables = ["stg_source_record", "quarantine_record", "match_candidate", "id_mapping_history", "mapping_conflict",
                  "unresolved_mapping", "source_key_status", "late_arrival_log"]
        return {t: self.scalar(f"SELECT count(*) FROM {t}") for t in tables}

    def fingerprints(self) -> dict[str, tuple[int, str]]:
        con = db.connect(self.cfg)
        try:
            return report.fingerprints(con)
        finally:
            con.close()

    def checks(self, determinism: bool = True) -> dict[str, report.CheckResult]:
        con = db.connect(self.cfg)
        try:
            results = report.hard_checks(con, self.cfg, determinism=determinism)
        finally:
            con.close()
        return {c.name.split(".")[0]: c for c in results}

    def assert_checks_pass(self, determinism: bool = True) -> None:
        failed = [c for c in self.checks(determinism).values() if c.status == report.FAIL]
        assert not failed, failed


@pytest.fixture
def ws(tmp_path: Path) -> Workspace:
    return Workspace(tmp_path)


@pytest.fixture(autouse=True)
def _quiet_logs(monkeypatch):
    import logging

    metrics.get_logger()
    metrics.set_console_level(logging.CRITICAL)
    monkeypatch.delenv("IDMAP_FAIL_AT_STAGE", raising=False)
    yield
