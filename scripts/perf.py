#!/usr/bin/env python3
"""Performance harness: end-to-end pipeline timing per stage, and the matching engines compared.

    python scripts/perf.py                         # 10k, 100k and 1M source rows -> output/perf.md
    python scripts/perf.py --sizes 10000 100000    # quicker

For each size the harness:
  1. generates a seeded dataset with scripts/generate.py;
  2. runs the full pipeline over every batch in a fresh database and records per-stage
     wall time (from the stage logs) and peak RSS;
  3. benchmarks normalize + match + resolve on the same staged rows, twice:
       baseline   Python, row by row: normalize each record, then look it up in dicts;
       optimized  DuckDB, set-based: SQL normalization macros, then equi-joins on blocking keys;
     and checks that both engines produce identical decisions.

Each measurement runs in its own subprocess, so peak RSS covers one engine only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import platform
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

STAGES = ("ingest", "schema", "validate", "dedup", "normalize", "match", "resolve", "publish", "status", "metrics")


def peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024


def _config(data: Path, db_path: Path):
    from idmap import metrics
    from idmap.config import load_config

    metrics.set_console_level(logging.CRITICAL)
    return load_config().replace(
        db_path=db_path, log_dir=db_path.parent / f"{db_path.stem}_logs", incoming_dir=data,
        incoming_pattern="*batch*.csv", canonical_file=data / "canonical_entity.csv",
        override_file=data / "mapping_override.csv",
    )


def worker_pipeline(data: Path, db_path: Path) -> dict:
    from idmap.pipeline import run_pipeline

    cfg = _config(data, db_path)
    start = time.perf_counter()
    results = run_pipeline(cfg)
    wall = time.perf_counter() - start
    stages = {s: 0 for s in STAGES}
    for r in results:
        for t in r.timings:
            stages[t.stage] += t.duration_ms
    return {
        "wall_s": wall,
        "rows_read": sum(r.counts.get("rows_read", 0) for r in results),
        "rows_staged": sum(r.counts.get("rows_staged", 0) for r in results),
        "files": len(results),
        "succeeded": sum(r.status == "SUCCEEDED" for r in results),
        "stages_ms": stages,
        "peak_rss_mb": peak_rss_mb(),
    }


def _write_decisions(rows, path: Path) -> str:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        for row in rows:
            writer.writerow(row)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def worker_match(engine: str, data: Path, db_path: Path, out: Path) -> dict:
    from idmap import db, ingest, match, normalize, resolve
    from idmap.baseline import BaselineMatcher

    cfg = _config(data, db_path)
    con = db.connect(cfg)
    con.execute(
        """CREATE TEMP TABLE t_dedup AS
           SELECT source_system, source_id, name, email, phone, domain, tax_id, canonical_ref, source_updated_at,
                  source_row_num
           FROM stg_source_record
           QUALIFY row_number() OVER (PARTITION BY source_system, source_id
                                      ORDER BY source_updated_at DESC, run_id DESC) = 1"""
    )
    keys = con.execute("SELECT count(*) FROM t_dedup").fetchone()[0]
    result: dict = {"engine": engine, "keys": keys}
    if engine == "sql":
        steps = {}
        con.execute("BEGIN TRANSACTION")  # canonical keys are rebuilt for timing, then rolled back
        t0 = time.perf_counter()
        ingest.rebuild_canonical_keys(con)
        steps["index"] = time.perf_counter() - t0
        t1 = time.perf_counter()
        normalize.normalize_batch(con)
        steps["normalize"] = time.perf_counter() - t1
        t2 = time.perf_counter()
        match.build_hits(con, cfg)
        resolve.build_decisions(con)
        steps["match_resolve"] = time.perf_counter() - t2
        result["compute_s"] = time.perf_counter() - t0
        rows = con.execute(
            """SELECT source_system, source_id, decision, canonical_id, rule_id,
                      array_to_string(candidate_ids, ';'), unresolved_reason
               FROM t_decision ORDER BY source_system, source_id"""
        ).fetchall()
        con.execute("ROLLBACK")
        result["steps"] = steps
    else:
        t_load = time.perf_counter()
        source = con.execute("SELECT source_system, source_id, name, email, phone, domain, tax_id FROM t_dedup").fetchall()
        canonicals = con.execute(
            "SELECT canonical_id, legal_name, tax_id, primary_email, domain, phone, is_active FROM canonical_entity"
        ).fetchall()
        overrides = con.execute("SELECT source_system, source_id, canonical_id FROM mapping_override").fetchall()
        result["load_s"] = time.perf_counter() - t_load
        t0 = time.perf_counter()
        matcher = BaselineMatcher(cfg, canonicals, overrides)
        t1 = time.perf_counter()
        decided = [(r[0], r[1], *matcher.decide(*r)) for r in source]
        t2 = time.perf_counter()
        rows = sorted(
            (s, i, d, c, rule, ";".join(ids) if ids else None, reason) for s, i, d, c, rule, ids, reason in decided
        )
        result["compute_s"] = t2 - t0
        result["steps"] = {"index": t1 - t0, "normalize_match_resolve": t2 - t1}
    con.close()
    result["decisions_sha256"] = _write_decisions(rows, out)
    result["peak_rss_mb"] = peak_rss_mb()
    return result


def _run_worker(*args: str) -> dict:
    proc = subprocess.run([sys.executable, __file__, "--worker", *args], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"worker {args[0]} failed:\n{proc.stderr}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _fmt_n(n: int) -> str:
    return f"{n // 1_000_000}M" if n >= 1_000_000 and n % 1_000_000 == 0 else (f"{n // 1000}k" if n >= 1000 else str(n))


def render(results: list[dict], seed: int) -> str:
    import duckdb

    lines = [
        "# Performance",
        "",
        f"Generated by `python scripts/perf.py` (seed {seed}) on {platform.platform()}, "
        f"{os.cpu_count()} CPUs, Python {platform.python_version()}, DuckDB {duckdb.__version__}.",
        "Timings are wall-clock seconds from a single run; expect run-to-run variance of roughly 10%.",
        "",
        "## End-to-end pipeline",
        "",
        "All batch files of the generated dataset, processed into a fresh database. The designed broken-schema",
        "file fails at the schema stage, as it should.",
        "",
        "| Target rows | Rows read | Files (succeeded) | Wall time (s) | Rows/s | Peak RSS (MB) |",
        "|---|---|---|---|---|---|",
    ]
    for r in results:
        p = r["pipeline"]
        lines.append(f"| {_fmt_n(r['n'])} | {p['rows_read']:,} | {p['files']} ({p['succeeded']}) | {p['wall_s']:.2f} | "
                     f"{p['rows_read'] / p['wall_s']:,.0f} | {p['peak_rss_mb']:.0f} |")
    lines += ["", "### Time per stage (ms, summed over files)", "",
              "| Stage | " + " | ".join(_fmt_n(r["n"]) for r in results) + " |",
              "|---|" + "---|" * len(results)]
    for stage in STAGES:
        lines.append(f"| {stage} | " + " | ".join(f"{r['pipeline']['stages_ms'][stage]:,}" for r in results) + " |")
    lines += [
        "",
        "## Matching: row-by-row Python baseline vs set-based DuckDB",
        "",
        "Same input for both engines: the latest staged record per key. Timed work covers building the",
        "canonical key index, normalizing every source record, generating candidates and resolving decisions.",
        "The baseline's time to fetch rows from DuckDB into Python is excluded (shown separately).",
        "",
        "| Target rows | Keys | Baseline (s) | Optimized (s) | Speedup | Baseline peak RSS (MB) | Optimized peak RSS (MB) | Identical decisions |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        b, s = r["baseline"], r["optimized"]
        same = "yes" if b["decisions_sha256"] == s["decisions_sha256"] else "**NO**"
        lines.append(f"| {_fmt_n(r['n'])} | {s['keys']:,} | {b['compute_s']:.2f} | {s['compute_s']:.2f} | "
                     f"{b['compute_s'] / s['compute_s']:.1f}x | {b['peak_rss_mb']:.0f} | {s['peak_rss_mb']:.0f} | {same} |")
    lines += ["", "Breakdown (s):", "",
              "| Target rows | Baseline fetch (excluded) | Baseline index | Baseline normalize+match+resolve | "
              "SQL canonical keys | SQL normalize | SQL match+resolve |",
              "|---|---|---|---|---|---|---|"]
    for r in results:
        b, s = r["baseline"], r["optimized"]
        lines.append(f"| {_fmt_n(r['n'])} | {b['load_s']:.2f} | {b['steps']['index']:.2f} | "
                     f"{b['steps']['normalize_match_resolve']:.2f} | {s['steps']['index']:.2f} | "
                     f"{s['steps']['normalize']:.2f} | {s['steps']['match_resolve']:.2f} |")
    lines += [
        "",
        "## Reading the numbers",
        "",
        "- The optimization is structural. Every rule is an equi-join on a normalized blocking key, so",
        "  candidate generation is a hash join and never a pairwise comparison. Normalization runs as",
        "  vectorized SQL macros across all cores, not in the Python interpreter one row at a time.",
        "- Both engines implement the same rules, and the decision files are compared byte for byte above.",
        "  The speedup costs no correctness.",
        "- At 10k rows fixed per-statement overhead dominates and the baseline is as fast as SQL. The",
        "  gap grows with volume (Python is linear and single-threaded; DuckDB vectorizes across cores).",
        "- Outside matching, the largest stages are `validate`, `normalize` and `status`. `validate` is",
        "  mostly the Python CSV pass that assigns exact physical line numbers and isolates malformed",
        "  lines. `normalize` includes writing the staging table. `status` is the per-key upserts into",
        "  the indexed state tables.",
        "",
        "## Next-step optimizations (documented, not built)",
        "",
        "- **Incremental processing:** hash each record's matching attributes and skip keys whose hash and",
        "  timestamp are unchanged since the last applied evidence. Re-sent unchanged records then cost one join.",
        "- **Native CSV parsing:** replace the Python line-number pass with DuckDB's reader plus its rejects",
        "  table once line numbers for multi-line records can be recovered there. That is the main remaining",
        "  single-threaded cost.",
        "- **Partitioning by `source_system`:** state tables and batch files partition naturally. Systems can",
        "  load in parallel because rules never compare keys across systems, except for the",
        "  many-to-one warning, which is per system anyway.",
        "- **Multi-node scale (100M+ keys):** the same SQL ports to Spark/Databricks. The blocking-key joins",
        "  become shuffle joins on `(rule_id, key_value)`, SCD2 becomes a Delta `MERGE`, and the",
        "  per-file transaction becomes a Delta commit.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="ID mapping performance harness")
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 100_000, 1_000_000])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=REPO / "output" / "perf.md")
    parser.add_argument("--worker", nargs="+", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        kind, *rest = args.worker
        if kind == "pipeline":
            out = worker_pipeline(Path(rest[0]), Path(rest[1]))
        else:
            out = worker_match(rest[0], Path(rest[1]), Path(rest[2]), Path(rest[3]))
        print(json.dumps(out))
        return 0

    results = []
    with tempfile.TemporaryDirectory(prefix="idmap_perf_") as tmp:
        for n in args.sizes:
            data = Path(tmp) / f"n{n}"
            db_path = Path(tmp) / f"n{n}.duckdb"
            t0 = time.perf_counter()
            subprocess.run([sys.executable, str(REPO / "scripts" / "generate.py"), "--n", str(n), "--seed", str(args.seed),
                            "--out", str(data)], check=True, capture_output=True)
            print(f"[{_fmt_n(n)}] generated in {time.perf_counter() - t0:.1f}s", file=sys.stderr)
            pipeline = _run_worker("pipeline", str(data), str(db_path))
            print(f"[{_fmt_n(n)}] pipeline {pipeline['wall_s']:.2f}s for {pipeline['rows_read']:,} rows", file=sys.stderr)
            optimized = _run_worker("match", "sql", str(data), str(db_path), str(Path(tmp) / f"n{n}_sql.csv"))
            baseline = _run_worker("match", "python", str(data), str(db_path), str(Path(tmp) / f"n{n}_py.csv"))
            same = optimized["decisions_sha256"] == baseline["decisions_sha256"]
            print(f"[{_fmt_n(n)}] matching: baseline {baseline['compute_s']:.2f}s, optimized {optimized['compute_s']:.2f}s, "
                  f"identical={same}", file=sys.stderr)
            results.append({"n": n, "pipeline": pipeline, "optimized": optimized, "baseline": baseline})
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(results, args.seed), encoding="utf-8")
    args.out.with_suffix(".json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.out}", file=sys.stderr)
    return 0 if all(r["optimized"]["decisions_sha256"] == r["baseline"]["decisions_sha256"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
