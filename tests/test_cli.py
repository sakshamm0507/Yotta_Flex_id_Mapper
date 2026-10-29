"""CLI smoke tests: commands, exit codes, and the end-to-end flow on the committed sample."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from idmap.cli import cli

REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "data" / "sample"


def _base(tmp_path: Path) -> list[str]:
    return ["-q", "--db", str(tmp_path / "idmap.duckdb"), "--log-dir", str(tmp_path / "logs")]


def _run_args() -> list[str]:
    return ["run", "--incoming", str(SAMPLE), "--pattern", "*batch*.csv", "--canonical", str(SAMPLE / "canonical_entity.csv"),
            "--overrides", str(SAMPLE / "mapping_override.csv")]


def test_run_exit_codes_and_rerun(tmp_path):
    runner = CliRunner()
    strict = runner.invoke(cli, _base(tmp_path) + _run_args())
    assert strict.exit_code == 1, strict.output          # the designed broken-schema file FAILED
    assert "FAILED" in strict.output and "SUCCEEDED" in strict.output
    again = runner.invoke(cli, _base(tmp_path) + _run_args() + ["--allow-failures"])
    assert again.exit_code == 0
    assert again.output.count("SKIPPED") == 5           # checksums already processed


def test_status_report_export(tmp_path):
    runner = CliRunner()
    runner.invoke(cli, _base(tmp_path) + _run_args() + ["--allow-failures"])
    status = runner.invoke(cli, _base(tmp_path) + ["status", "--limit", "3"])
    assert status.exit_code == 0 and "run_id" in status.output
    out = tmp_path / "report.md"
    rep = runner.invoke(cli, _base(tmp_path) + ["report", "--expected-dir", str(REPO / "data" / "expected"), "--out", str(out)])
    assert rep.exit_code == 0, rep.output
    text = out.read_text()
    assert "Verdict: **PASS**" in text and "Spec conformance" in text and "100.00%" in text
    exp = runner.invoke(cli, _base(tmp_path) + ["export", "--out", str(tmp_path / "tables")])
    assert exp.exit_code == 0 and (tmp_path / "tables" / "source_key_status.csv").is_file()


def test_report_fails_on_violation(tmp_path):
    import duckdb

    runner = CliRunner()
    runner.invoke(cli, _base(tmp_path) + _run_args() + ["--allow-failures"])
    con = duckdb.connect(str(tmp_path / "idmap.duckdb"))
    con.execute("UPDATE pipeline_run SET rows_read = rows_read + 1 WHERE status = 'SUCCEEDED'")
    con.close()
    rep = runner.invoke(cli, _base(tmp_path) + ["report", "--skip-determinism", "--out", str(tmp_path / "r.md")])
    assert rep.exit_code == 1 and "[FAIL]" in rep.output


def test_missing_database_is_a_clean_error(tmp_path):
    result = CliRunner().invoke(cli, _base(tmp_path) + ["status"])
    assert result.exit_code != 0 and "database not found" in result.output
