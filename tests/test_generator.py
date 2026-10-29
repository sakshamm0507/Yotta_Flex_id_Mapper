"""Generator determinism, plus end-to-end agreement between the pipeline and the generator's labels."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from idmap import db, report
from idmap.config import load_config
from idmap.pipeline import run_pipeline

REPO = Path(__file__).resolve().parents[1]
GENERATE = REPO / "scripts" / "generate.py"


def generate(out: Path, *args: str) -> None:
    subprocess.run([sys.executable, str(GENERATE), "--out", str(out), *args], check=True, capture_output=True)


def digest(directory: Path) -> dict[str, str]:
    return {p.relative_to(directory).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(directory.rglob("*")) if p.is_file()}


def run_dataset(tmp: Path, data: Path, expected: Path) -> tuple[dict, dict]:
    cfg = load_config().replace(
        db_path=tmp / "idmap.duckdb", log_dir=tmp / "logs", incoming_dir=data, incoming_pattern="*batch*.csv",
        canonical_file=data / "canonical_entity.csv", override_file=data / "mapping_override.csv",
    )
    results = run_pipeline(cfg)
    con = db.connect(cfg)
    try:
        checks = {c.name.split(".")[0]: c.status for c in report.hard_checks(con, cfg)}
        quality = report.quality_metrics(con, expected)
    finally:
        con.close()
    statuses = {r.source_file.rsplit("/", 1)[-1]: r.status for r in results}
    return {"checks": checks, "statuses": statuses}, quality


def test_same_seed_same_bytes(tmp_path):
    generate(tmp_path / "a", "--n", "3000", "--seed", "11")
    generate(tmp_path / "b", "--n", "3000", "--seed", "11")
    generate(tmp_path / "c", "--n", "3000", "--seed", "12")
    assert digest(tmp_path / "a") == digest(tmp_path / "b")
    assert digest(tmp_path / "a") != digest(tmp_path / "c")


def test_committed_sample_is_reproducible(tmp_path):
    generate(tmp_path / "sample", "--sample", "--seed", "7", "--expected-out", str(tmp_path / "expected"))
    committed = {**{f"sample/{k}": v for k, v in digest(REPO / "data" / "sample").items() if k.endswith(".csv")},
                 **{f"expected/{k}": v for k, v in digest(REPO / "data" / "expected").items()}}
    fresh = {**{f"sample/{k}": v for k, v in digest(tmp_path / "sample").items()},
             **{f"expected/{k}": v for k, v in digest(tmp_path / "expected").items()}}
    assert fresh == committed


def _assert_full_agreement(outcome: dict, quality: dict, expected: Path) -> None:
    assert all(status != "FAIL" for status in outcome["checks"].values()), outcome["checks"]
    assert outcome["checks"]["7"] == "PASS"
    assert outcome["statuses"].pop("broken_schema_batch.csv") == "FAILED"
    assert set(outcome["statuses"].values()) == {"SUCCEEDED"}
    agree, total = quality["spec_agreement"]
    assert agree == total, quality["disagreements"]
    assert all(caught == n for _, n, caught in quality["quarantine_expected"])
    summary = json.loads((expected / "expected_summary.json").read_text())
    assert quality["duplicates_dropped"] == summary["exact_duplicate_rows"]
    assert quality["history"][3] == summary["late_arrival_records"]
    open_m2o = sum(n for kind, status, n, _ in quality["conflicts_by_type"]
                   if kind == "MANY_TO_ONE_SAME_SYSTEM" and status == "OPEN")
    assert open_m2o == summary["many_to_one_keys"]


def test_committed_sample_end_to_end(tmp_path):
    expected = REPO / "data" / "expected"
    outcome, quality = run_dataset(tmp_path, REPO / "data" / "sample", expected)
    _assert_full_agreement(outcome, quality, expected)
    scenarios = {s for s, _, _ in quality["by_scenario"]}
    for required in ("one_to_many_email", "inactive_only", "no_match", "dirty_name+override", "remap",
                     "late_arrival", "conflict_keeps_mapping", "unresolved_then_mapped", "duplicate_account",
                     "weak_rule_false_positive", "boundary_unicode_name", "boundary_whitespace_only"):
        assert required in scenarios, required


@pytest.mark.parametrize("seed", [3, 2026])
def test_generated_dataset_end_to_end(tmp_path, seed):
    data = tmp_path / "data"
    generate(data, "--n", "6000", "--seed", str(seed))
    outcome, quality = run_dataset(tmp_path, data, data / "expected")
    _assert_full_agreement(outcome, quality, data / "expected")
    assert quality["precision"] < 1.0 and quality["recall"] < 1.0   # designed traps register
