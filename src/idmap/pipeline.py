"""Orchestration: process batch files one at a time, one transaction per file.

    ingest (checksum, registry)  <- outside the transaction
    BEGIN
      schema -> validate -> dedup -> normalize -> match -> resolve -> publish -> status -> metrics
    COMMIT  (pipeline_run is set to SUCCEEDED inside the same transaction)

The pipeline_run row is written before BEGIN, so a failed run is still
recorded. On any exception the transaction rolls back, leaving no partial
writes. The run row is then marked FAILED with the failing stage and error,
and the file stays unprocessed, so the next `idmap run` retries it from scratch.

`IDMAP_FAIL_AT_STAGE=<stage>` raises at the start of that stage (failure-injection tests).
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from idmap import db, ingest, match, metrics, normalize, publish, resolve, validate
from idmap.config import Config

STAGES = ("ingest", "schema", "validate", "dedup", "normalize", "match", "resolve", "publish", "status", "metrics")
FAIL_ENV = "IDMAP_FAIL_AT_STAGE"


class InjectedFailure(RuntimeError):
    """Raised by IDMAP_FAIL_AT_STAGE to exercise rollback and retry."""


class ReconciliationError(RuntimeError):
    """In-run invariant broken: the batch is rolled back instead of committed."""


@dataclass
class RunResult:
    run_id: str
    source_file: str
    status: str
    counts: dict[str, int] = field(default_factory=dict)
    failure_reason: str | None = None
    status_detail: str | None = None
    timings: list[metrics.StageTiming] = field(default_factory=list)
    log_file: Path | None = None


def _maybe_fail(stage: str) -> None:
    if os.environ.get(FAIL_ENV, "").strip().lower() == stage:
        raise InjectedFailure(f"injected failure at stage '{stage}' ({FAIL_ENV})")


def mark_abandoned_runs(con: duckdb.DuckDBPyConnection) -> int:
    """A RUNNING row at startup belongs to a process that died; DuckDB's file lock
    guarantees no other writer is active."""
    abandoned = con.execute("SELECT count(*) FROM pipeline_run WHERE status = 'RUNNING'").fetchone()[0]
    if abandoned:
        con.execute(
            """UPDATE pipeline_run
               SET status = 'FAILED', ended_at = ?, failed_stage = 'unknown',
                   failure_reason = 'Abandoned: process exited before the run finished'
               WHERE status = 'RUNNING'""",
            [metrics.utc_now()],
        )
    return abandoned


def _start_run(con, run_id: str, source_file: str, forced: bool, reference_sha: str | None) -> None:
    seq = con.execute("SELECT coalesce(max(run_seq), 0) + 1 FROM pipeline_run").fetchone()[0]
    con.execute(
        """INSERT INTO pipeline_run (run_id, run_seq, source_file, reference_sha256, forced, status, started_at)
           VALUES (?, ?, ?, ?, ?, 'RUNNING', ?)""",
        [run_id, seq, source_file, reference_sha, forced, metrics.utc_now()],
    )


def _clear_previous_batch_rows(con, file_sha: str, run_id: str) -> None:
    """Forced rerun: batch-scoped rows of earlier runs of this same content are replaced."""
    for table in db.BATCH_TABLES:
        con.execute(
            f"""DELETE FROM {table} WHERE run_id IN (
                    SELECT run_id FROM pipeline_run WHERE file_sha256 = ? AND run_id <> ?)""",
            [file_sha, run_id],
        )


def process_file(
    con: duckdb.DuckDBPyConnection,
    cfg: Config,
    path: Path,
    force: bool = False,
    reference_sha: str | None = None,
) -> RunResult:
    source_file = ingest.display_path(path, cfg.root)
    run_id = metrics.new_run_id()
    log = metrics.RunLog(run_id, source_file)
    _start_run(con, run_id, source_file, force, reference_sha)
    result = RunResult(run_id, source_file, "RUNNING")
    stage = "ingest"
    with metrics.run_log_file(cfg.log_dir, run_id) as log_path:
        result.log_file = log_path
        log.info("run started", forced=force)
        try:
            with log.stage("ingest") as ctx:
                _maybe_fail("ingest")
                if not path.is_file():
                    raise FileNotFoundError(f"batch file not found: {path}")
                file_sha = ingest.file_sha256(path)
                con.execute("UPDATE pipeline_run SET file_sha256 = ? WHERE run_id = ?", [file_sha, run_id])
                previous = ingest.succeeded_run_for(con, file_sha)
                ctx["file_sha256"] = file_sha
            if previous and not force:
                detail = f"checksum already processed by {previous[0]} ({previous[1]})"
                con.execute(
                    """UPDATE pipeline_run SET status = 'SKIPPED', ended_at = ?, status_detail = ?,
                           duration_ms = date_diff('millisecond', started_at, ?)
                       WHERE run_id = ?""",
                    [metrics.utc_now(), detail, metrics.utc_now(), run_id],
                )
                log.info("run skipped", status="SKIPPED", detail=detail)
                result.status, result.status_detail, result.timings = "SKIPPED", detail, log.timings
                return result

            stage = "commit"  # stages tag their own exceptions; anything else failed at COMMIT
            with tempfile.TemporaryDirectory(prefix="idmap_") as tmp, db.transaction(con):
                counts = _run_stages(con, cfg, path, run_id, source_file, file_sha, log, Path(tmp), result)
            result.status, result.counts = "SUCCEEDED", counts
            log.info("run succeeded", status="SUCCEEDED", **counts)
        except Exception as exc:  # noqa: BLE001 - every failure is recorded on the run row
            stage = getattr(exc, "_idmap_stage", stage)
            reason = f"{stage}: {type(exc).__name__}: {exc}"
            now = metrics.utc_now()
            con.execute(
                """UPDATE pipeline_run
                   SET status = 'FAILED', ended_at = ?, duration_ms = date_diff('millisecond', started_at, ?),
                       failed_stage = ?, failure_reason = ?
                   WHERE run_id = ?""",
                [now, now, stage, reason, run_id],
            )
            sha = con.execute("SELECT file_sha256 FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()[0]
            if sha is not None and ingest.succeeded_run_for(con, sha) is None:
                con.execute(
                    """INSERT INTO processed_file VALUES (?, ?, ?, 'FAILED', ?)
                       ON CONFLICT (file_sha256) DO UPDATE SET source_file = excluded.source_file,
                           run_id = excluded.run_id, status = 'FAILED', processed_at = excluded.processed_at""",
                    [sha, source_file, run_id, now],
                )
            log.error("run failed", status="FAILED", failed_stage=stage, failure_reason=reason)
            result.status, result.failure_reason = "FAILED", reason
        result.timings = log.timings
    return result


def _run_stages(con, cfg, path, run_id, source_file, file_sha, log, tmp: Path, result: RunResult) -> dict[str, int]:
    def stage(name: str, rows_in: int | None = None):
        _maybe_fail(name)
        return log.stage(name, rows_in)

    current = "schema"
    try:
        with stage("schema") as ctx:
            schema = validate.check_schema(ingest.read_header(path))
            for warning in schema.warnings:
                log.warning("schema warning", stage="schema", detail=warning)
            ctx["schema_warnings"] = len(schema.warnings)
            _clear_previous_batch_rows(con, file_sha, run_id)

        current = "validate"
        with stage("validate") as ctx:
            parsed = ingest.parse_batch(path, schema, tmp / "wellformed.csv")
            ingest.load_parsed(con, parsed, tmp / "wellformed.csv")
            v = validate.validate_rows(con, cfg, run_id, source_file, file_sha)
            ctx["rows_in"], ctx["rows_out"] = parsed.rows_read, v["valid"]
            ctx.update(malformed=v["malformed"], invalid=v["invalid"])
            validate.check_reject_rate(cfg, parsed.rows_read, v["malformed"] + v["invalid"])

        current = "dedup"
        with stage("dedup", v["valid"]) as ctx:
            d = validate.deduplicate(con, run_id, source_file, file_sha)
            ctx["rows_out"] = d["unique_keys"]
            ctx.update(exact_duplicates=d["exact_duplicates"], superseded=d["superseded"])

        current = "normalize"
        with stage("normalize", d["unique_keys"]) as ctx:
            staged = normalize.normalize_batch(con)
            con.execute(
                """INSERT INTO stg_source_record
                   SELECT ?, ?, source_row_num, source_system, source_id, name, email, phone, domain, tax_id,
                          canonical_ref, source_updated_at, tax_id_norm, email_norm, phone_norm, name_norm,
                          domain_norm, ?
                   FROM t_stage""",
                [run_id, source_file, metrics.utc_now()],
            )
            ctx["rows_out"] = staged

        current = "match"
        with stage("match", staged) as ctx:
            hits = match.build_hits(con, cfg)
            ctx["rows_out"] = match.record_candidates(con, run_id)
            ctx["hits_including_inactive"] = hits

        current = "resolve"
        with stage("resolve", staged) as ctx:
            decisions = resolve.build_decisions(con)
            ctx["rows_out"] = sum(decisions.values())
            ctx.update({k.lower(): v for k, v in decisions.items()})

        current = "publish"
        with stage("publish", staged) as ctx:
            outcome = publish.build_eval(con)
            changes = publish.publish(con, run_id)
            ctx["rows_out"] = changes["mappings_inserted"]
            ctx.update(**changes, late_arrivals=outcome["late_arrivals"])

        current = "status"
        with stage("status", staged) as ctx:
            status = resolve.record_outcomes(con, run_id, file_sha)
            ctx["rows_out"] = staged - outcome["late_arrivals"]
            ctx.update(status)

        current = "metrics"
        with stage("metrics") as ctx:
            quarantined = con.execute(
                "SELECT count(*) FROM quarantine_record WHERE run_id = ?", [run_id]
            ).fetchone()[0]
            counts = {
                "rows_read": parsed.rows_read,
                "rows_staged": staged,
                "rows_quarantined": quarantined,
                "duplicates_dropped": d["exact_duplicates"],
                "mapped": outcome["mapped"],
                "conflicts": outcome["conflicts"],
                "unresolved": outcome["unresolved"],
                "mappings_inserted": changes["mappings_inserted"],
                "mappings_closed": changes["mappings_closed"],
                "late_arrivals": outcome["late_arrivals"],
                "schema_warnings": len(schema.warnings),
                "many_to_one_open": status["many_to_one_open"],
            }
            _reconcile(counts)
            now = metrics.utc_now()
            con.execute(
                f"""UPDATE pipeline_run SET status = 'SUCCEEDED', ended_at = ?,
                        duration_ms = date_diff('millisecond', started_at, ?),
                        {", ".join(f"{k} = ?" for k in counts)}
                    WHERE run_id = ?""",
                [now, now, *counts.values(), run_id],
            )
            con.execute(
                """INSERT INTO processed_file VALUES (?, ?, ?, 'SUCCEEDED', ?)
                   ON CONFLICT (file_sha256) DO UPDATE SET source_file = excluded.source_file,
                       run_id = excluded.run_id, status = 'SUCCEEDED', processed_at = excluded.processed_at""",
                [file_sha, source_file, run_id, now],
            )
            for table in ("t_raw", "t_malformed", "t_typed", "t_valid", "t_dedup", "t_stage", "t_hits", "t_decision", "t_eval"):
                con.execute(f"DROP TABLE IF EXISTS {table}")
            ctx["rows_out"] = staged
        return counts
    except Exception as exc:
        exc._idmap_stage = current  # type: ignore[attr-defined]
        raise


def _reconcile(c: dict[str, int]) -> None:
    if c["rows_read"] != c["rows_staged"] + c["rows_quarantined"] + c["duplicates_dropped"]:
        raise ReconciliationError(
            f"rows_read {c['rows_read']} != staged {c['rows_staged']} + quarantined {c['rows_quarantined']} "
            f"+ duplicates {c['duplicates_dropped']}"
        )
    if c["rows_staged"] != c["mapped"] + c["conflicts"] + c["unresolved"] + c["late_arrivals"]:
        raise ReconciliationError(
            f"rows_staged {c['rows_staged']} != mapped + conflicts + unresolved + late arrivals"
        )


def run_pipeline(cfg: Config, files: list[Path] | None = None, force: bool = False) -> list[RunResult]:
    """Process `files` (default: every batch in the incoming dir, in filename order)."""
    con = db.connect(cfg)
    try:
        abandoned = mark_abandoned_runs(con)
        if abandoned:
            metrics.get_logger().warning(f"marked {abandoned} abandoned RUNNING run(s) as FAILED", extra={"run_id": "-"})
        loaded = ingest.load_reference(con, cfg)
        if loaded.get("reloaded"):
            metrics.get_logger().info(
                f"reference loaded: {loaded['canonical_rows']} canonical, {loaded['override_rows']} override rows",
                extra={"run_id": "-"},
            )
        reference_sha = ingest.reference_fingerprint(cfg)
        targets = files if files is not None else ingest.discover_files(cfg.incoming_dir, cfg.incoming_pattern)
        return [process_file(con, cfg, Path(p), force=force, reference_sha=reference_sha) for p in targets]
    finally:
        con.close()
