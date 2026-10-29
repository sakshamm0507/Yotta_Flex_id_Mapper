"""Command-line entry point: `python -m idmap <command>` (or `idmap` once installed)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from idmap import db, metrics, report
from idmap.config import ConfigError, load_config
from idmap.ingest import ReferenceDataError
from idmap.pipeline import run_pipeline


def _resolve(path: str | None) -> Path | None:
    return None if path is None else Path(path).resolve()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--config", "config_path", type=click.Path(dir_okay=False), help="Pipeline YAML (default config/pipeline.yaml).")
@click.option("--db", "db_path", type=click.Path(dir_okay=False), help="DuckDB file (overrides paths.db).")
@click.option("--log-dir", type=click.Path(file_okay=False), help="Run log directory (overrides paths.log_dir).")
@click.option("-q", "--quiet", is_flag=True, help="Only warnings and errors on stderr.")
@click.pass_context
def cli(ctx: click.Context, config_path: str | None, db_path: str | None, log_dir: str | None, quiet: bool) -> None:
    """Map source-system identifiers to canonical IDs with deterministic rules."""
    try:
        cfg = load_config(config_path)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    changes = {}
    if db_path:
        changes["db_path"] = _resolve(db_path)
    if log_dir:
        changes["log_dir"] = _resolve(log_dir)
    ctx.obj = cfg.replace(**changes) if changes else cfg
    metrics.get_logger()
    if quiet:
        metrics.set_console_level(logging.WARNING)


@cli.command()
@click.option("--incoming", type=click.Path(file_okay=False), help="Directory of batch CSVs (overrides paths.incoming_dir).")
@click.option("--pattern", help="Glob for batch files (overrides paths.incoming_pattern).")
@click.option("--file", "files", multiple=True, type=click.Path(dir_okay=False), help="Process only these files (repeatable).")
@click.option("--canonical", type=click.Path(dir_okay=False), help="Canonical master CSV (overrides reference.canonical_file).")
@click.option("--overrides", type=click.Path(dir_okay=False), help="Manual crosswalk CSV (overrides reference.override_file).")
@click.option("--force", is_flag=True, help="Reprocess files whose checksum already succeeded (idempotent).")
@click.option("--allow-failures", is_flag=True, help="Exit 0 even if a file FAILED (it is still recorded).")
@click.pass_obj
def run(cfg, incoming, pattern, files, canonical, overrides, force, allow_failures) -> None:
    """Process every batch not yet SUCCEEDED, in filename order, one transaction per file."""
    changes = {}
    if incoming:
        changes["incoming_dir"] = _resolve(incoming)
    if pattern:
        changes["incoming_pattern"] = pattern
    if canonical:
        changes["canonical_file"] = _resolve(canonical)
    if overrides:
        changes["override_file"] = _resolve(overrides)
    cfg = cfg.replace(**changes) if changes else cfg
    try:
        results = run_pipeline(cfg, files=[Path(f) for f in files] if files else None, force=force)
    except (ReferenceDataError, FileNotFoundError) as exc:
        raise click.ClickException(str(exc)) from exc
    if not results:
        click.echo(f"No batch files matching '{cfg.incoming_pattern}' in {cfg.incoming_dir}")
        return
    rows = [[r.source_file, r.status, r.counts.get("rows_read", ""), r.counts.get("rows_staged", ""),
             r.counts.get("rows_quarantined", ""), r.counts.get("mapped", ""), r.counts.get("conflicts", ""),
             r.counts.get("unresolved", ""), r.counts.get("late_arrivals", ""),
             (r.failure_reason or r.status_detail or "")[:90]] for r in results]
    click.echo()
    click.echo(report.text_table(["file", "status", "read", "staged", "quarantined", "mapped", "conflict",
                                  "unresolved", "late", "detail"], rows))
    failed = [r for r in results if r.status == "FAILED"]
    if failed:
        click.echo(f"\n{len(failed)} file(s) FAILED; they stay unprocessed and are retried by the next run.", err=True)
        if not allow_failures:
            sys.exit(1)


@cli.command()
@click.option("--limit", default=20, show_default=True, help="Number of most recent runs.")
@click.pass_obj
def status(cfg, limit: int) -> None:
    """Show the most recent pipeline runs."""
    if not cfg.db_path.exists():
        raise click.ClickException(f"database not found: {cfg.db_path} (run `idmap run` first)")
    con = db.connect(cfg)
    try:
        rows = report.run_rows(con, limit)
    finally:
        con.close()
    headers = ["#", "run_id", "file", "status", "read", "staged", "quar", "dupes", "mapped", "conflict",
               "unres", "late", "ins", "closed", "warn", "ms"]
    click.echo(report.text_table(headers, [[*r[:16]] for r in rows]))
    for r in rows:
        if r[16]:
            click.echo(f"  {r[1]}: {r[16]}")


@cli.command("report")
@click.option("--expected-dir", type=click.Path(file_okay=False), help="Ground-truth directory (overrides quality.expected_dir).")
@click.option("--out", type=click.Path(dir_okay=False), help="Report path (overrides paths.report_file).")
@click.option("--skip-determinism", is_flag=True, help="Skip the rebuild-and-compare check (faster).")
@click.pass_obj
def report_cmd(cfg, expected_dir, out, skip_determinism) -> None:
    """Run reconciliation checks, compute quality metrics and write the Markdown report."""
    if not cfg.db_path.exists():
        raise click.ClickException(f"database not found: {cfg.db_path} (run `idmap run` first)")
    out_path = _resolve(out) or cfg.report_file
    expected = _resolve(expected_dir) if expected_dir else cfg.expected_dir
    ok, checks = report.build_report(cfg, out_path, expected, determinism=not skip_determinism)
    for c in checks:
        click.echo(f"[{c.status}] {c.name}: {c.detail}")
    click.echo(f"\nReport written to {out_path}")
    if not ok:
        click.echo("One or more hard checks FAILED.", err=True)
        sys.exit(1)


@cli.command()
@click.option("--out", required=True, type=click.Path(file_okay=False), help="Directory for one CSV per table.")
@click.pass_obj
def export(cfg, out) -> None:
    """Export every pipeline table to CSV (sorted, for review and diffing)."""
    for path in report.export_tables(cfg, Path(out)):
        click.echo(f"wrote {path}")


def main() -> None:
    cli(prog_name="idmap")


if __name__ == "__main__":
    main()
