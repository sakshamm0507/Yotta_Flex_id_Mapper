"""Ingest: file discovery, checksums, the processed-file registry, CSV parsing
and reference-data loading.

Source CSVs are parsed with Python's csv module. That gives an exact physical
line number for every record, quoted multi-line fields included, and catches
structurally broken lines (MALFORMED_ROW) before they reach DuckDB. Well-formed
records are rewritten to a temporary CSV with their line number attached, then
bulk-loaded with DuckDB's native reader.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from idmap.config import Config
from idmap.db import transaction
from idmap.match import rule_keys_sql
from idmap.metrics import utc_now
from idmap.validate import CONTRACT_COLUMNS, SchemaCheck

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

RAW_TABLE = "t_raw"
MALFORMED_TABLE = "t_malformed"
# NUL, or a byte that was not valid UTF-8 (decoded with surrogateescape to U+DC80..U+DCFF).
_BAD_CHARS = re.compile("[\x00\udc80-\udcff]")


class ReferenceDataError(ValueError):
    """The canonical master or override file is unusable."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(directory: Path, pattern: str) -> list[Path]:
    """Batch files in filename order; that order is the processing order."""
    if not directory.is_dir():
        return []
    return sorted((p for p in directory.glob(pattern) if p.is_file()), key=lambda p: p.name)


def display_path(path: Path, root: Path) -> str:
    """Path stored in lineage columns: repo-relative when possible."""
    resolved = path.resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def succeeded_run_for(con: duckdb.DuckDBPyConnection, sha: str) -> tuple[str, str] | None:
    row = con.execute(
        "SELECT run_id, source_file FROM processed_file WHERE file_sha256 = ? AND status = 'SUCCEEDED'",
        [sha],
    ).fetchone()
    return None if row is None else (row[0], row[1])


def read_header(path: Path) -> list[str] | None:
    with path.open(newline="", encoding="utf-8-sig", errors="surrogateescape") as fh:
        for row in csv.reader(fh):
            if row:
                return row
    return None


@dataclass
class ParsedBatch:
    rows_read: int = 0
    rows_wellformed: int = 0
    malformed: list[tuple[int, str, str]] = field(default_factory=list)


def _printable(value: str) -> str:
    """Make undecodable bytes visible (\\xNN) so the payload is valid JSON text."""
    return value.encode("utf-8", "surrogateescape").decode("utf-8", "backslashreplace")


def _has_bad_bytes(fields: list[str]) -> bool:
    return _BAD_CHARS.search("".join(fields)) is not None


def parse_batch(path: Path, schema: SchemaCheck, out_csv: Path) -> ParsedBatch:
    """Split records into well-formed (written to `out_csv`) and malformed."""
    result = ParsedBatch()
    width = len(schema.header)
    positions = [schema.positions.get(col) for col in CONTRACT_COLUMNS]
    with (
        path.open(newline="", encoding="utf-8-sig", errors="surrogateescape") as src,
        out_csv.open("w", newline="", encoding="utf-8") as dst,
    ):
        reader = csv.reader(src)
        writer = csv.writer(dst, lineterminator="\n")
        writer.writerow(["source_row_num", *CONTRACT_COLUMNS])
        header_seen = False
        prev_line = 0
        while True:
            start_line = prev_line + 1
            try:
                fields = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                prev_line = reader.line_num
                result.rows_read += 1
                result.malformed.append((start_line, json.dumps({"error": str(exc)}), f"unparseable line: {exc}"))
                continue
            prev_line = reader.line_num
            if not fields:
                continue  # blank line: not a record
            if not header_seen:
                header_seen = True
                continue
            result.rows_read += 1
            if len(fields) != width:
                payload = json.dumps({"fields": [_printable(f) for f in fields]}, ensure_ascii=False)
                detail = f"expected {width} columns, found {len(fields)}"
                result.malformed.append((start_line, payload, detail))
                continue
            if _has_bad_bytes(fields):
                payload = json.dumps({"fields": [_printable(f) for f in fields]}, ensure_ascii=False)
                result.malformed.append((start_line, payload, "invalid UTF-8 or NUL byte"))
                continue
            writer.writerow([start_line] + ["" if pos is None else fields[pos] for pos in positions])
            result.rows_wellformed += 1
    return result


def load_parsed(con: duckdb.DuckDBPyConnection, parsed: ParsedBatch, clean_csv: Path) -> None:
    """Load the well-formed rows (all VARCHAR) and the malformed list into temp tables."""
    columns = ", ".join(["'source_row_num': 'BIGINT'"] + [f"'{c}': 'VARCHAR'" for c in CONTRACT_COLUMNS])
    path_literal = str(clean_csv).replace("'", "''")
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE {RAW_TABLE} AS
            SELECT * FROM read_csv('{path_literal}', header = true, auto_detect = false,
                                   delim = ',', quote = '"', escape = '"',
                                   columns = {{{columns}}})"""
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {MALFORMED_TABLE} "
        "(source_row_num BIGINT, raw_payload VARCHAR, reject_detail VARCHAR)"
    )
    if parsed.malformed:
        con.executemany(f"INSERT INTO {MALFORMED_TABLE} VALUES (?, ?, ?)", parsed.malformed)


# Reference data ---------------------------------------------------------------

CANONICAL_COLUMNS = ("canonical_id", "legal_name", "tax_id", "primary_email", "domain", "phone", "is_active", "created_at")
OVERRIDE_COLUMNS = ("source_system", "source_id", "canonical_id", "reason", "created_at")


def _read_reference_csv(path: Path, columns: tuple[str, ...]) -> str:
    if not path.is_file():
        raise ReferenceDataError(f"reference file not found: {path}")
    header = [h.strip().lower() for h in (read_header(path) or [])]
    missing = [c for c in columns if c not in header]
    if missing:
        raise ReferenceDataError(f"{path.name}: missing column(s) {missing}")
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in header)
    path_literal = str(path).replace("'", "''")
    return (
        f"read_csv('{path_literal}', header = true, auto_detect = false, delim = ',', quote = '\"', "
        f"escape = '\"', columns = {{{cols}}})"
    )


def reference_fingerprint(cfg: Config) -> str:
    parts = [file_sha256(cfg.canonical_file)]
    if cfg.override_file and cfg.override_file.is_file():
        parts.append(file_sha256(cfg.override_file))
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def load_reference(con: duckdb.DuckDBPyConnection, cfg: Config) -> dict[str, int | bool]:
    """Refresh canonical_entity / mapping_override / canonical_key when the files changed."""
    canonical_sha = file_sha256(cfg.canonical_file) if cfg.canonical_file.is_file() else None
    if canonical_sha is None:
        raise ReferenceDataError(f"canonical master not found: {cfg.canonical_file}")
    override_sha = (
        file_sha256(cfg.override_file) if cfg.override_file and cfg.override_file.is_file() else "none"
    )
    loaded = dict(con.execute("SELECT reference_name, file_sha256 FROM reference_load").fetchall())
    if loaded.get("canonical_entity") == canonical_sha and loaded.get("mapping_override") == override_sha:
        return {"reloaded": False}

    canonical_src = _read_reference_csv(cfg.canonical_file, CANONICAL_COLUMNS)
    now = utc_now()
    with transaction(con):
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE t_canonical AS
                SELECT idm_clean(canonical_id) AS canonical_id,
                       idm_clean(legal_name) AS legal_name,
                       idm_clean(tax_id) AS tax_id,
                       idm_clean(primary_email) AS primary_email,
                       idm_clean(domain) AS domain,
                       idm_clean(phone) AS phone,
                       TRY_CAST(lower(idm_clean(is_active)) AS BOOLEAN) AS is_active,
                       idm_parse_ts(created_at) AS created_at
                FROM {canonical_src}"""
        )
        problems = con.execute(
            """SELECT
                   count(*) FILTER (WHERE canonical_id IS NULL OR legal_name IS NULL),
                   count(*) FILTER (WHERE is_active IS NULL),
                   count(*) - count(DISTINCT canonical_id)
               FROM t_canonical"""
        ).fetchone()
        if any(problems):
            raise ReferenceDataError(
                f"{cfg.canonical_file.name}: {problems[0]} row(s) missing id/name, "
                f"{problems[1]} with invalid is_active, {problems[2]} duplicate canonical_id(s)"
            )
        con.execute("DELETE FROM canonical_entity")
        con.execute("INSERT INTO canonical_entity SELECT * FROM t_canonical")
        canonical_rows = con.execute("SELECT count(*) FROM canonical_entity").fetchone()[0]

        con.execute("DELETE FROM mapping_override")
        override_rows = 0
        if override_sha != "none":
            override_src = _read_reference_csv(cfg.override_file, OVERRIDE_COLUMNS)
            con.execute(
                f"""INSERT INTO mapping_override
                    SELECT lower(idm_clean(source_system)), idm_clean(source_id), idm_clean(canonical_id),
                           idm_clean(reason), idm_parse_ts(created_at)
                    FROM {override_src}"""
            )
            unknown = con.execute(
                """SELECT list(o.canonical_id ORDER BY o.canonical_id) FROM mapping_override o
                   LEFT JOIN canonical_entity c USING (canonical_id) WHERE c.canonical_id IS NULL"""
            ).fetchone()[0]
            if unknown:
                raise ReferenceDataError(f"mapping_override references unknown canonical_id(s): {unknown[:5]}")
            override_rows = con.execute("SELECT count(*) FROM mapping_override").fetchone()[0]

        rebuild_canonical_keys(con)
        con.execute("DELETE FROM reference_load")
        con.executemany(
            "INSERT INTO reference_load VALUES (?, ?, ?, ?, ?)",
            [
                ["canonical_entity", str(cfg.canonical_file), canonical_sha, canonical_rows, now],
                ["mapping_override", str(cfg.override_file or ""), override_sha, override_rows, now],
            ],
        )
        con.execute("DROP TABLE t_canonical")
    return {"reloaded": True, "canonical_rows": canonical_rows, "override_rows": override_rows}


def rebuild_canonical_keys(con: duckdb.DuckDBPyConnection) -> None:
    """Blocking index: the canonical side of every attribute rule key (see match.py)."""
    con.execute("DELETE FROM canonical_key")
    con.execute(
        f"""INSERT INTO canonical_key (canonical_id, is_active, rule_id, key_value)
            WITH n AS (
                SELECT canonical_id, is_active,
                       idm_tax_norm(tax_id) AS tax_id_norm,
                       idm_email_norm(primary_email) AS email_norm,
                       idm_name_norm(legal_name) AS name_norm,
                       idm_domain_norm(domain) AS domain_norm,
                       idm_phone_norm(phone) AS phone_norm
                FROM canonical_entity
            )
            {rule_keys_sql("n", "canonical_id, is_active")}"""
    )
