"""Reconciliation checks, quality metrics against ground truth, and the Markdown report.

Hard checks are assertions about the database; if any fails, `idmap report` exits non-zero.
Quality metrics compare results with labels the generator writes separately
from the inputs (expected/). Soft thresholds from the config raise WARN only.
"""

from __future__ import annotations

import csv
import json
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import duckdb

from idmap import db, ingest, metrics
from idmap.config import Config
from idmap.pipeline import run_pipeline
from idmap.validate import REASON_CODES

PASS, FAIL, SKIP, WARN, OK = "PASS", "FAIL", "SKIP", "WARN", "OK"


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str


# Fingerprints --------------------------------------------------------------------
# Wall-clock audit columns are excluded. Run ids are time-based, so each one is
# replaced by the checksum of the file that run processed. Lineage still counts:
# a row created by a different input gives a different fingerprint.
FINGERPRINT_SPEC: dict[str, tuple[list[str], list[str]]] = {
    "id_mapping_history": (
        ["mapping_id", "source_system", "source_id", "canonical_id", "match_rule", "valid_from", "valid_to", "is_current"],
        ["created_run_id", "closed_run_id"],
    ),
    "source_key_status": (
        ["source_system", "source_id", "status", "status_reason", "canonical_id", "last_source_updated_at"],
        ["last_run_id"],
    ),
    "mapping_conflict": (
        ["conflict_id", "conflict_type", "source_system", "source_id", "candidate_canonical_ids", "rule_id",
         "has_existing_mapping", "existing_canonical_id", "status"],
        ["first_detected_run_id", "last_detected_run_id", "resolved_run_id"],
    ),
    "unresolved_mapping": (
        ["source_system", "source_id", "reason", "has_existing_mapping", "status", "attempt_count"],
        ["first_seen_run_id", "last_seen_run_id", "resolved_run_id"],
    ),
    "late_arrival_log": (
        ["source_system", "source_id", "record_updated_at", "latest_applied_updated_at", "current_valid_from",
         "current_canonical_id", "proposed_status", "proposed_canonical_id", "action"],
        ["run_id"],
    ),
    "quarantine_record": (
        ["quarantine_id", "source_file", "source_row_num", "raw_payload", "reject_reason_code", "reject_detail"],
        ["run_id"],
    ),
    "match_candidate": (
        ["source_system", "source_id", "canonical_id", "rule_id", "rule_priority", "matched_value"],
        ["run_id"],
    ),
    "stg_source_record": (
        ["source_file", "source_row_num", "source_system", "source_id", "name", "email", "phone", "domain", "tax_id",
         "canonical_ref", "source_updated_at", "tax_id_norm", "email_norm", "phone_norm", "name_norm", "domain_norm"],
        ["run_id"],
    ),
}


def fingerprints(con: duckdb.DuckDBPyConnection) -> dict[str, tuple[int, str]]:
    con.execute(
        "CREATE OR REPLACE TEMP TABLE t_run_sha AS SELECT run_id, coalesce(file_sha256, '?') AS sha FROM pipeline_run"
    )
    out = {}
    for table, (columns, run_columns) in FINGERPRINT_SPEC.items():
        joins, exprs = [], [f"coalesce(t.{c}::VARCHAR, '<null>')" for c in columns]
        for i, rc in enumerate(run_columns):
            joins.append(f"LEFT JOIN t_run_sha r{i} ON r{i}.run_id = t.{rc}")
            exprs.append(f"coalesce(r{i}.sha, '<null>')")
        row = con.execute(
            f"""SELECT count(*), coalesce(sha256(string_agg(h, '' ORDER BY h)), 'empty') FROM (
                    SELECT md5(concat_ws('|', {", ".join(exprs)})) AS h FROM {table} t {" ".join(joins)}
                )"""
        ).fetchone()
        out[table] = (row[0], row[1])
    con.execute("DROP TABLE t_run_sha")
    return out


# Hard checks ---------------------------------------------------------------------

def _rows(con, sql: str, params: list | None = None) -> list[tuple]:
    return con.execute(sql, params or []).fetchall()


def _check(name: str, failures: list, ok_detail: str, fmt=lambda f: str(f)) -> CheckResult:
    if failures:
        shown = "; ".join(fmt(f) for f in failures[:5])
        more = f" (+{len(failures) - 5} more)" if len(failures) > 5 else ""
        return CheckResult(name, FAIL, f"{len(failures)} violation(s): {shown}{more}")
    return CheckResult(name, PASS, ok_detail)


def hard_checks(con: duckdb.DuckDBPyConnection, cfg: Config, determinism: bool = True) -> list[CheckResult]:
    results: list[CheckResult] = []
    n_runs = db.scalar(con, "SELECT count(*) FROM pipeline_run WHERE status = 'SUCCEEDED'")

    bad = _rows(con, """SELECT run_id, rows_read, rows_staged, rows_quarantined, duplicates_dropped FROM pipeline_run
                        WHERE status = 'SUCCEEDED'
                          AND rows_read <> rows_staged + rows_quarantined + duplicates_dropped ORDER BY run_seq""")
    results.append(_check("1. rows_read = staged + quarantined + duplicates (every run)", bad,
                          f"holds for {n_runs} succeeded run(s)", lambda r: f"{r[0]}: {r[1]} != {r[2]}+{r[3]}+{r[4]}"))

    bad = _rows(con, """SELECT run_id FROM pipeline_run WHERE status = 'SUCCEEDED'
                        AND rows_staged <> mapped + conflicts + unresolved + late_arrivals ORDER BY run_seq""")
    results.append(_check("1b. staged = mapped + conflicts + unresolved + late arrivals (every run)", bad,
                          f"holds for {n_runs} succeeded run(s)"))

    # Stored rows match the run counters, for the latest run of each file content
    # (a forced rerun replaces the batch rows of earlier runs of the same file).
    bad = _rows(con, """WITH latest AS (
                            SELECT * FROM pipeline_run WHERE status = 'SUCCEEDED'
                            QUALIFY row_number() OVER (PARTITION BY file_sha256 ORDER BY run_seq DESC) = 1)
                        SELECT l.run_id, l.rows_staged, (SELECT count(*) FROM stg_source_record s WHERE s.run_id = l.run_id),
                               l.rows_quarantined, (SELECT count(*) FROM quarantine_record q WHERE q.run_id = l.run_id)
                        FROM latest l
                        WHERE l.rows_staged <> (SELECT count(*) FROM stg_source_record s WHERE s.run_id = l.run_id)
                           OR l.rows_quarantined <> (SELECT count(*) FROM quarantine_record q WHERE q.run_id = l.run_id)""")
    results.append(_check("1c. run counters match stored staging/quarantine rows", bad, "counters match table rows",
                          lambda r: f"{r[0]}: staged {r[1]} vs {r[2]}, quarantined {r[3]} vs {r[4]}"))

    staged_keys, status_rows, by_status = con.execute(
        """SELECT (SELECT count(*) FROM (SELECT DISTINCT source_system, source_id FROM stg_source_record)),
                  (SELECT count(*) FROM source_key_status),
                  (SELECT count(*) FROM source_key_status WHERE status IN ('MAPPED', 'CONFLICT', 'UNRESOLVED'))"""
    ).fetchone()
    detail = f"{staged_keys} distinct staged keys = {by_status} MAPPED+CONFLICT+UNRESOLVED"
    ok = staged_keys == status_rows == by_status
    results.append(CheckResult("2. distinct staged keys = MAPPED + CONFLICT + UNRESOLVED", PASS if ok else FAIL,
                               detail if ok else f"staged keys {staged_keys}, status rows {status_rows}, "
                                                 f"in known statuses {by_status}"))

    bad = _rows(con, """SELECT source_system, source_id, count(*) FROM id_mapping_history WHERE is_current
                        GROUP BY ALL HAVING count(*) > 1""")
    results.append(_check("3. at most one current mapping per business key", bad,
                          f"{db.scalar(con, 'SELECT count(*) FROM id_mapping_current')} current mappings, all unique"))

    bad = _rows(con, """SELECT m.source_system, m.source_id, m.canonical_id, c.is_active
                        FROM id_mapping_current m LEFT JOIN canonical_entity c USING (canonical_id)
                        WHERE c.canonical_id IS NULL OR NOT c.is_active""")
    results.append(_check("4. every current canonical_id exists and is active", bad, "all current targets valid",
                          lambda r: f"{r[0]}/{r[1]} -> {r[2]} ({'missing' if r[3] is None else 'inactive'})"))

    bad = _rows(con, """SELECT a.source_system, a.source_id, a.mapping_id, b.mapping_id
                        FROM id_mapping_history a JOIN id_mapping_history b
                          ON a.source_system = b.source_system AND a.source_id = b.source_id AND a.mapping_id < b.mapping_id
                        WHERE a.valid_from < coalesce(b.valid_to, 'infinity'::TIMESTAMP)
                          AND b.valid_from < coalesce(a.valid_to, 'infinity'::TIMESTAMP)
                          AND a.valid_from < coalesce(a.valid_to, 'infinity'::TIMESTAMP)
                          AND b.valid_from < coalesce(b.valid_to, 'infinity'::TIMESTAMP)
                        UNION ALL
                        SELECT source_system, source_id, mapping_id, 'shape' FROM id_mapping_history
                        WHERE (is_current AND (valid_to IS NOT NULL OR closed_run_id IS NOT NULL))
                           OR (NOT is_current AND (valid_to IS NULL OR closed_run_id IS NULL OR valid_to < valid_from))""")
    results.append(_check("5. no overlapping [valid_from, valid_to) intervals per key", bad,
                          f"{db.scalar(con, 'SELECT count(*) FROM id_mapping_history')} history rows, no overlaps",
                          lambda r: f"{r[0]}/{r[1]}"))

    codes = ", ".join(f"'{c}'" for c in REASON_CODES)
    bad = _rows(con, f"""SELECT quarantine_id FROM quarantine_record
                         WHERE run_id IS NULL OR source_file IS NULL OR source_row_num IS NULL
                            OR reject_reason_code IS NULL OR reject_reason_code NOT IN ({codes})""")
    results.append(_check("6. every quarantined row carries run_id, source_file, source_row_num, reason", bad,
                          f"{db.scalar(con, 'SELECT count(*) FROM quarantine_record')} quarantined rows traceable"))

    bad = _rows(con, """SELECT k.source_system, k.source_id FROM source_key_status k
                        LEFT JOIN id_mapping_current m USING (source_system, source_id)
                        WHERE k.canonical_id IS DISTINCT FROM m.canonical_id
                           OR (k.status = 'MAPPED' AND m.canonical_id IS NULL)""")
    results.append(_check("8. source_key_status agrees with id_mapping_current", bad, "status and history consistent",
                          lambda r: f"{r[0]}/{r[1]}"))

    bad = _rows(con, """SELECT k.source_system, k.source_id, k.status FROM source_key_status k
                        LEFT JOIN mapping_conflict c ON c.conflict_type = 'ONE_TO_MANY' AND c.status = 'OPEN'
                             AND c.source_system = k.source_system AND c.source_id = k.source_id
                        LEFT JOIN unresolved_mapping u ON u.status = 'OPEN'
                             AND u.source_system = k.source_system AND u.source_id = k.source_id
                        WHERE (k.status = 'CONFLICT') <> (c.conflict_id IS NOT NULL)
                           OR (k.status = 'UNRESOLVED') <> (u.source_id IS NOT NULL)""")
    results.append(_check("9. open conflicts/unresolved match per-key status", bad, "open work queues consistent",
                          lambda r: f"{r[0]}/{r[1]} ({r[2]})"))

    dupes = []
    for table, key in (
        ("stg_source_record", "run_id, source_system, source_id"),
        ("quarantine_record", "quarantine_id"),
        ("match_candidate", "run_id, source_system, source_id, rule_id, canonical_id"),
        ("late_arrival_log", "run_id, source_system, source_id"),
    ):
        n = db.scalar(con, f"SELECT count(*) FROM (SELECT {key} FROM {table} GROUP BY ALL HAVING count(*) > 1)")
        if n:
            dupes.append(f"{table}: {n}")
    results.append(_check("10. logical keys of batch tables are unique", dupes, "no duplicate logical keys"))

    if determinism:
        results.append(determinism_check(con, cfg))
    else:
        results.append(CheckResult("7. determinism: fresh rebuild reproduces mapping tables", SKIP, "disabled by flag"))
    return sorted(results, key=lambda r: _check_order(r.name))


def _check_order(name: str) -> tuple[int, str]:
    head = name.split(".")[0]
    digits = "".join(ch for ch in head if ch.isdigit())
    return (int(digits) if digits else 99, head)


def determinism_check(con: duckdb.DuckDBPyConnection, cfg: Config) -> CheckResult:
    """Replay every succeeded run, in order, into a scratch database and compare fingerprints."""
    name = "7. determinism: fresh rebuild reproduces mapping tables"
    runs = _rows(con, "SELECT source_file, file_sha256, reference_sha256 FROM pipeline_run "
                      "WHERE status = 'SUCCEEDED' ORDER BY run_seq")
    if not runs:
        return CheckResult(name, SKIP, "no succeeded runs to replay")
    # Replay against the reference files the runs actually loaded (recorded in reference_load).
    loaded = dict(_rows(con, "SELECT reference_name, source_file FROM reference_load"))
    if "canonical_entity" in loaded:
        cfg = cfg.replace(canonical_file=Path(loaded["canonical_entity"]),
                          override_file=Path(loaded["mapping_override"]) if loaded.get("mapping_override") else None)
    try:
        reference_sha = ingest.reference_fingerprint(cfg)
    except FileNotFoundError:
        return CheckResult(name, SKIP, "reference files not available for replay")
    if any(r[2] != reference_sha for r in runs):
        return CheckResult(name, SKIP, "reference data changed during run history; replay would not be comparable")
    files = []
    for source_file, sha, _ in runs:
        path = Path(source_file)
        path = path if path.is_absolute() else cfg.root / path
        if not path.is_file() or ingest.file_sha256(path) != sha:
            return CheckResult(name, SKIP, f"input changed or missing, cannot replay: {source_file}")
        files.append(path)

    logger = metrics.get_logger()
    previous_levels = [h.level for h in logger.handlers]
    metrics.set_console_level(logging.CRITICAL)  # the replay's own warnings are noise here
    try:
        with tempfile.TemporaryDirectory(prefix="idmap_replay_") as tmp:
            replay_cfg = cfg.replace(db_path=Path(tmp) / "replay.duckdb", log_dir=Path(tmp) / "logs")
            results = run_pipeline(replay_cfg, files=files, force=True)
            failed = [r for r in results if r.status != "SUCCEEDED"]
            if failed:
                return CheckResult(name, FAIL, f"replay run failed: {failed[0].failure_reason}")
            replay_con = db.connect(replay_cfg)
            try:
                replayed = fingerprints(replay_con)
            finally:
                replay_con.close()
    finally:
        for handler, level in zip(logger.handlers, previous_levels):
            handler.setLevel(level)
    original = fingerprints(con)
    diffs = [t for t in FINGERPRINT_SPEC if original[t] != replayed[t]]
    if diffs:
        return CheckResult(name, FAIL, "differs in " + ", ".join(f"{t} {original[t][0]} vs {replayed[t][0]} rows" for t in diffs))
    return CheckResult(name, PASS, f"replayed {len(files)} run(s); {len(FINGERPRINT_SPEC)} tables identical "
                                   f"(history fingerprint {original['id_mapping_history'][1][:12]})")


# Quality metrics -------------------------------------------------------------------

def _load_csv_table(con, name: str, path: Path) -> bool:
    if not path.is_file():
        return False
    literal = str(path).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM read_csv('{literal}', header = true, all_varchar = true)")
    return True


def _ratio(num: int, den: int) -> float | None:
    return None if not den else num / den


def quality_metrics(con: duckdb.DuckDBPyConnection, expected_dir: Path | None) -> dict:
    out: dict = {"labels": False}
    totals = con.execute(
        """SELECT count(*), count(*) FILTER (WHERE status = 'MAPPED'), count(*) FILTER (WHERE status = 'CONFLICT'),
                  count(*) FILTER (WHERE status = 'UNRESOLVED') FROM source_key_status"""
    ).fetchone()
    out["keys"], out["mapped"], out["conflict"], out["unresolved"] = totals
    out["match_rate"], out["conflict_rate"], out["unresolved_rate"] = (
        _ratio(totals[1], totals[0]), _ratio(totals[2], totals[0]), _ratio(totals[3], totals[0]))
    effective = """SELECT * FROM pipeline_run WHERE status = 'SUCCEEDED'
                   QUALIFY row_number() OVER (PARTITION BY file_sha256 ORDER BY run_seq DESC) = 1"""
    read, quarantined, dropped = con.execute(
        f"SELECT coalesce(sum(rows_read), 0), coalesce(sum(rows_quarantined), 0), coalesce(sum(duplicates_dropped), 0) FROM ({effective})"
    ).fetchone()
    out["rows_read"], out["rows_quarantined"], out["duplicates_dropped"] = int(read), int(quarantined), int(dropped)
    out["quarantine_rate"] = _ratio(int(quarantined), int(read))
    out["quarantine_by_code"] = _rows(con, """SELECT reject_reason_code, count(*) FROM quarantine_record
                                              GROUP BY ALL ORDER BY count(*) DESC, 1""")
    out["unresolved_by_reason"] = _rows(con, """SELECT reason, count(*), count(*) FILTER (WHERE has_existing_mapping)
                                                FROM unresolved_mapping WHERE status = 'OPEN' GROUP BY ALL ORDER BY 2 DESC, 1""")
    out["conflicts_by_type"] = _rows(con, """SELECT conflict_type, status, count(*), count(*) FILTER (WHERE has_existing_mapping)
                                             FROM mapping_conflict GROUP BY ALL ORDER BY 1, 2""")
    out["history"] = con.execute(
        """SELECT count(*), count(*) FILTER (WHERE NOT is_current),
                  (SELECT count(*) FROM (SELECT 1 FROM id_mapping_history GROUP BY source_system, source_id HAVING count(*) > 1)),
                  (SELECT count(*) FROM late_arrival_log),
                  (SELECT count(*) FROM late_arrival_log WHERE proposed_canonical_id IS DISTINCT FROM current_canonical_id)
           FROM id_mapping_history"""
    ).fetchone()
    out["by_system"] = _rows(con, """SELECT source_system, count(*), count(*) FILTER (WHERE status = 'MAPPED'),
                                            count(*) FILTER (WHERE status = 'CONFLICT'), count(*) FILTER (WHERE status = 'UNRESOLVED')
                                     FROM source_key_status GROUP BY ALL ORDER BY 1""")
    out["by_rule"] = _rows(con, "SELECT match_rule, count(*) FROM id_mapping_current GROUP BY ALL ORDER BY 2 DESC")
    ref = con.execute(
        """WITH latest AS (
               SELECT * FROM stg_source_record
               QUALIFY row_number() OVER (PARTITION BY source_system, source_id
                                          ORDER BY source_updated_at DESC, run_id DESC) = 1)
           SELECT count(*), count(*) FILTER (WHERE l.canonical_ref = m.canonical_id)
           FROM latest l JOIN id_mapping_current m USING (source_system, source_id)
           WHERE l.canonical_ref IS NOT NULL"""
    ).fetchone()
    out["canonical_ref"] = ref

    if expected_dir is None or not _load_csv_table(con, "t_expected", expected_dir / "expected_labels.csv"):
        return out
    out["labels"] = True
    out["expected_dir"] = expected_dir
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_quality AS
           WITH q AS (
               SELECT DISTINCT lower(trim(raw_payload->>'source_system')) AS source_system,
                               trim(raw_payload->>'source_id') AS source_id
               FROM quarantine_record
           )
           SELECT e.*, coalesce(k.status, CASE WHEN q.source_id IS NOT NULL THEN 'QUARANTINED' ELSE 'ABSENT' END) AS actual_status,
                  m.canonical_id AS actual_canonical_id, m.match_rule AS actual_rule,
                  tc.is_active AS true_is_active
           FROM t_expected e
           LEFT JOIN source_key_status k ON k.source_system = e.source_system AND k.source_id = e.source_id
           LEFT JOIN id_mapping_current m ON m.source_system = e.source_system AND m.source_id = e.source_id
           LEFT JOIN q ON q.source_system = e.source_system AND q.source_id = e.source_id
           LEFT JOIN canonical_entity tc ON tc.canonical_id = nullif(e.true_canonical_id, '')"""
    )
    out["labeled_keys"] = db.scalar(con, "SELECT count(*) FROM t_quality")
    out["spec_agreement"] = con.execute(
        """SELECT count(*) FILTER (WHERE expected_status = actual_status
                                    AND nullif(expected_canonical_id, '') IS NOT DISTINCT FROM actual_canonical_id),
                  count(*) FROM t_quality"""
    ).fetchone()
    out["confusion"] = _rows(con, "SELECT expected_status, actual_status, count(*) FROM t_quality GROUP BY ALL ORDER BY ALL")
    out["disagreements"] = _rows(
        con,
        """SELECT source_system, source_id, scenario, expected_status, nullif(expected_canonical_id, ''),
                  actual_status, actual_canonical_id
           FROM t_quality
           WHERE NOT (expected_status = actual_status
                      AND nullif(expected_canonical_id, '') IS NOT DISTINCT FROM actual_canonical_id)
           ORDER BY ALL LIMIT 15""",
    )
    # Identity quality against the truth. Precision: of the keys holding a current mapping,
    # the share that point at the true entity. Recall: of the keys whose true entity is an
    # active canonical and that were not quarantined, the share mapped to that entity.
    tp, mapped_n, in_scope = con.execute(
        """SELECT count(*) FILTER (WHERE actual_canonical_id = nullif(true_canonical_id, '')),
                  count(*) FILTER (WHERE actual_canonical_id IS NOT NULL),
                  count(*) FILTER (WHERE true_is_active AND expected_status <> 'QUARANTINED')
           FROM t_quality"""
    ).fetchone()
    tp_in_scope = db.scalar(
        con,
        """SELECT count(*) FROM t_quality WHERE true_is_active AND expected_status <> 'QUARANTINED'
           AND actual_canonical_id = nullif(true_canonical_id, '')""",
    )
    precision, recall = _ratio(tp, mapped_n), _ratio(tp_in_scope, in_scope)
    f1 = None if not precision or not recall else 2 * precision * recall / (precision + recall)
    out["precision"], out["recall"], out["f1"] = precision, recall, f1
    out["precision_counts"], out["recall_counts"] = (tp, mapped_n), (tp_in_scope, in_scope)
    out["per_rule"] = _rows(
        con,
        """WITH p AS (
               SELECT actual_rule AS rule, count(*) AS mapped,
                      count(*) FILTER (WHERE actual_canonical_id = nullif(true_canonical_id, '')) AS correct
               FROM t_quality WHERE actual_rule IS NOT NULL GROUP BY 1),
           r AS (
               SELECT expected_rule AS rule, count(*) AS expected,
                      count(*) FILTER (WHERE actual_canonical_id = nullif(true_canonical_id, '')) AS found
               FROM t_quality
               WHERE nullif(expected_rule, '') IS NOT NULL AND true_is_active AND expected_status <> 'QUARANTINED'
               GROUP BY 1)
           SELECT coalesce(p.rule, r.rule), coalesce(p.mapped, 0), coalesce(p.correct, 0),
                  coalesce(r.expected, 0), coalesce(r.found, 0)
           FROM p FULL OUTER JOIN r ON p.rule = r.rule ORDER BY 1""",
    )
    out["by_scenario"] = _rows(
        con,
        """SELECT scenario, count(*),
                  count(*) FILTER (WHERE expected_status = actual_status
                                    AND nullif(expected_canonical_id, '') IS NOT DISTINCT FROM actual_canonical_id)
           FROM t_quality GROUP BY ALL ORDER BY 1""",
    )

    if _load_csv_table(con, "t_expected_q", expected_dir / "expected_quarantine.csv"):
        out["quarantine_expected"] = _rows(
            con,
            """WITH a AS (
                   SELECT regexp_extract(source_file, '[^/]+$') AS file, source_row_num::BIGINT AS row_num,
                          reject_reason_code AS code FROM quarantine_record)
               SELECT e.expected_reason_code, count(*),
                      count(*) FILTER (WHERE a.code = e.expected_reason_code)
               FROM t_expected_q e
               LEFT JOIN a ON a.file = e.source_file AND a.row_num = e.source_row_num::BIGINT
               GROUP BY ALL ORDER BY 1""",
        )
    summary_path = expected_dir / "expected_summary.json"
    if summary_path.is_file():
        out["expected_summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
    return out


# Rendering -------------------------------------------------------------------------

def md_table(headers: list[str], rows: list[list]) -> str:
    def cell(v) -> str:
        return ("" if v is None else str(v)).replace("|", "\\|").replace("\n", " ")

    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def text_table(headers: list[str], rows: list[list]) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    return "\n".join([fmt.format(*headers), fmt.format(*("-" * w for w in widths))] + [fmt.format(*r) for r in cells])


def pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def soft_checks(cfg: Config, q: dict) -> list[list]:
    t = cfg.thresholds
    rows = []

    def add(label: str, value: float | None, limit: float | None, higher_is_better: bool) -> None:
        if limit is None or value is None:
            return
        good = value >= limit if higher_is_better else value <= limit
        rows.append([label, pct(value), ("≥ " if higher_is_better else "≤ ") + pct(limit), OK if good else WARN])

    add("Precision", q.get("precision"), t.get("min_precision"), True)
    add("Recall", q.get("recall"), t.get("min_recall"), True)
    add("Conflict rate", q.get("conflict_rate"), t.get("max_conflict_rate"), False)
    add("Unresolved rate", q.get("unresolved_rate"), t.get("max_unresolved_rate"), False)
    add("Quarantine rate", q.get("quarantine_rate"), t.get("max_quarantine_rate"), False)
    return rows


RUN_COLUMNS = ["run_seq", "run_id", "source_file", "status", "rows_read", "rows_staged", "rows_quarantined",
               "duplicates_dropped", "mapped", "conflicts", "unresolved", "late_arrivals", "mappings_inserted",
               "mappings_closed", "schema_warnings", "duration_ms", "failure_reason", "status_detail"]


def run_rows(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> list[tuple]:
    sql = f"SELECT {', '.join(RUN_COLUMNS)} FROM pipeline_run ORDER BY run_seq DESC"
    rows = _rows(con, sql + (f" LIMIT {int(limit)}" if limit else ""))
    return list(reversed(rows))


def render(cfg: Config, checks: list[CheckResult], q: dict, runs: list[tuple]) -> str:
    failed = [c for c in checks if c.status == FAIL]
    verdict = "FAIL" if failed else "PASS"
    passed = sum(c.status == PASS for c in checks)
    lines = [
        "# ID Mapping: Reconciliation and Quality Report",
        "",
        f"- Generated: {metrics.utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"- Database: `{ingest.display_path(cfg.db_path, cfg.root)}`",
        f"- Ground truth: `{ingest.display_path(q['expected_dir'], cfg.root) if q.get('expected_dir') else 'not provided'}`",
        "",
        f"## Verdict: **{verdict}** ({passed}/{len(checks)} hard checks passed"
        + (f", {sum(c.status == SKIP for c in checks)} skipped" if any(c.status == SKIP for c in checks) else "")
        + ")",
        "",
        "## Hard checks",
        "",
        md_table(["Check", "Result", "Detail"], [[c.name, c.status, c.detail] for c in checks]),
        "",
        "## Pipeline runs",
        "",
        md_table(["#", "Run", "File", "Status", "Read", "Staged", "Quarantined", "Dupes", "Mapped", "Conflict",
                  "Unresolved", "Late", "Ins", "Closed", "Warn", "ms", "Failure / detail"],
                 [[*r[:16], r[16] or r[17] or ""] for r in runs]),
        "",
        "## Outcome summary",
        "",
        md_table(["Metric", "Value"], [
            ["Business keys", q["keys"]],
            ["MAPPED", f"{q['mapped']} ({pct(q['match_rate'])})"],
            ["CONFLICT", f"{q['conflict']} ({pct(q['conflict_rate'])})"],
            ["UNRESOLVED", f"{q['unresolved']} ({pct(q['unresolved_rate'])})"],
            ["Rows read (latest run per file)", q["rows_read"]],
            ["Rows quarantined", f"{q['rows_quarantined']} ({pct(q['quarantine_rate'])})"],
            ["Exact duplicates dropped", q["duplicates_dropped"]],
            ["History rows / closed versions", f"{q['history'][0]} / {q['history'][1]}"],
            ["Keys with more than one version", q["history"][2]],
            ["Late arrivals ignored (with different outcome)", f"{q['history'][3]} ({q['history'][4]})"],
            ["Legacy canonical_ref agreeing with mapping", f"{q['canonical_ref'][1]}/{q['canonical_ref'][0]}"
                                                           f" ({pct(_ratio(q['canonical_ref'][1], q['canonical_ref'][0]))})"],
        ]),
        "",
        "### By source system",
        "",
        md_table(["System", "Keys", "Mapped", "Conflict", "Unresolved"], [list(r) for r in q["by_system"]]),
        "",
        "### Current mappings by rule",
        "",
        md_table(["Rule", "Mappings"], [list(r) for r in q["by_rule"]]),
        "",
        "### Quarantine by reason code",
        "",
    ]
    expected_q = {r[0]: (r[1], r[2]) for r in q.get("quarantine_expected", [])}
    q_rows = []
    for code, n in q["quarantine_by_code"]:
        exp = expected_q.get(code)
        q_rows.append([code, n, pct(_ratio(n, q["rows_quarantined"])),
                       "" if exp is None else exp[0], "" if exp is None else f"{exp[1]}/{exp[0]}"])
    for code, (n_exp, matched) in expected_q.items():
        if code not in {r[0] for r in q["quarantine_by_code"]}:
            q_rows.append([code, 0, pct(0), n_exp, f"{matched}/{n_exp}"])
    lines += [md_table(["Reason", "Rows", "Share", "Expected", "Expected rows caught (same file+line+code)"], q_rows), ""]
    lines += ["### Open unresolved backlog", "",
              md_table(["Reason", "Keys", "Of which keep an earlier mapping"], [list(r) for r in q["unresolved_by_reason"]]),
              "", "### Conflicts", "",
              md_table(["Type", "Status", "Keys", "Of which keep an earlier mapping"], [list(r) for r in q["conflicts_by_type"]]),
              ""]

    lines += ["## Quality against ground truth", ""]
    if not q["labels"]:
        lines += ["No `expected_labels.csv` found; quality metrics skipped.", ""]
    else:
        agree, total = q["spec_agreement"]
        lines += [
            "Two views. **Spec conformance** compares each key's final status and canonical with what the rules",
            "should produce given the designed inputs (expected_status / expected_canonical_id). **Identity quality**",
            "compares mappings with the true entity (true_canonical_id). It shows the cost of deterministic,",
            "conflict-averse rules: designed conflicts and dirty names lower recall, and the weak phone+name",
            "rule lowers precision.",
            "",
            f"### Spec conformance: {agree}/{total} labeled keys agree ({pct(_ratio(agree, total))})",
            "",
        ]
        actual_cols = ["MAPPED", "CONFLICT", "UNRESOLVED", "QUARANTINED", "ABSENT"]
        matrix: dict[str, dict[str, int]] = {}
        for e, a, n in q["confusion"]:
            matrix.setdefault(e, {})[a] = n
        lines += [md_table(["expected \\ actual", *actual_cols],
                           [[e, *[matrix[e].get(a, 0) for a in actual_cols]] for e in sorted(matrix)]), ""]
        if q["disagreements"]:
            lines += ["Disagreements (first 15):", "",
                      md_table(["System", "Id", "Scenario", "Expected", "Expected canonical", "Actual", "Actual canonical"],
                               [list(r) for r in q["disagreements"]]), ""]
        lines += [
            "### Identity quality (vs true entity)",
            "",
            md_table(["Metric", "Value", "Counts"], [
                ["Precision", pct(q["precision"]), f"{q['precision_counts'][0]}/{q['precision_counts'][1]} current mappings correct"],
                ["Recall", pct(q["recall"]), f"{q['recall_counts'][0]}/{q['recall_counts'][1]} in-scope keys mapped to their entity"],
                ["F1", pct(q["f1"]), ""],
            ]),
            "",
            "Per rule. Precision is grouped by the rule that produced the current mapping. Recall is grouped by the rule",
            "that should decide the key, and counts how many of those keys ended up mapped to their true entity.",
            "",
            md_table(["Rule", "Mapped", "Correct", "Precision", "Expected to decide", "Mapped to truth", "Recall"],
                     [[r[0], r[1], r[2], pct(_ratio(r[2], r[1])), r[3], r[4], pct(_ratio(r[4], r[3]))] for r in q["per_rule"]]),
            "",
            "### Agreement by scenario",
            "",
            md_table(["Scenario", "Keys", "Agree", "Rate"], [[s, n, a, pct(_ratio(a, n))] for s, n, a in q["by_scenario"]]),
            "",
        ]
        summary = q.get("expected_summary")
        if summary:
            lines += ["### Designed volumes vs observed", "",
                      md_table(["Measure", "Designed", "Observed"], [
                          ["Exact duplicate rows dropped", summary.get("exact_duplicate_rows"), q["duplicates_dropped"]],
                          ["Late-arrival records", summary.get("late_arrival_records"), q["history"][3]],
                          ["Many-to-one keys (same system)", summary.get("many_to_one_keys"),
                           sum(r[2] for r in q["conflicts_by_type"] if r[0] == "MANY_TO_ONE_SAME_SYSTEM" and r[1] == "OPEN")],
                      ]), ""]
    soft = soft_checks(cfg, q)
    if soft:
        lines += ["## Soft thresholds (config `quality.thresholds`)", "",
                  md_table(["Metric", "Observed", "Threshold", "Result"], soft), ""]
    return "\n".join(lines).rstrip() + "\n"


def build_report(cfg: Config, out_path: Path, expected_dir: Path | None, determinism: bool = True) -> tuple[bool, list[CheckResult]]:
    con = db.connect(cfg)
    try:
        checks = hard_checks(con, cfg, determinism=determinism)
        q = quality_metrics(con, expected_dir)
        text = render(cfg, checks, q, run_rows(con))
    finally:
        con.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return all(c.status != FAIL for c in checks), checks


EXPORT_TABLES = (
    "pipeline_run", "processed_file", "canonical_entity", "mapping_override", "stg_source_record",
    "quarantine_record", "match_candidate", "id_mapping_history", "mapping_conflict", "unresolved_mapping",
    "source_key_status", "late_arrival_log",
)


def export_tables(cfg: Config, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = db.connect(cfg)
    written = []
    try:
        for table in EXPORT_TABLES:
            cursor = con.execute(f"SELECT * FROM {table} ORDER BY ALL")
            headers = [d[0] for d in cursor.description]
            path = out_dir / f"{table}.csv"
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh, lineterminator="\n")
                writer.writerow(headers)
                writer.writerows(cursor.fetchall())
            written.append(path)
    finally:
        con.close()
    return written
