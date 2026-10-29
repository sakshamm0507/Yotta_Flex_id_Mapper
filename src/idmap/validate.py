"""Schema contract, row validation and in-batch deduplication.

Each rejected row gets one reason code: the first rule it fails, checked in this order.

    MALFORMED_ROW > MISSING_REQUIRED_FIELD > UNKNOWN_SOURCE_SYSTEM > TYPE_CAST_FAILED
    > INVALID_EMAIL > INVALID_PHONE > INVALID_CANONICAL_REF

Valid rows are then deduplicated per business key (source_system, source_id).
Exact duplicates, identical after trimming, are dropped and counted. Of the
remaining versions the newest source_updated_at wins; on a tie the later line
in the file wins. Every older version is quarantined as DUPLICATE_KEY_SUPERSEDED.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from idmap.config import Config

REQUIRED_COLUMNS = ("source_system", "source_id", "source_updated_at")
OPTIONAL_COLUMNS = ("name", "email", "phone", "domain", "tax_id", "canonical_ref")
CONTRACT_COLUMNS = (
    "source_system",
    "source_id",
    "name",
    "email",
    "phone",
    "domain",
    "tax_id",
    "canonical_ref",
    "source_updated_at",
)

REASON_CODES = (
    "MALFORMED_ROW",
    "MISSING_REQUIRED_FIELD",
    "UNKNOWN_SOURCE_SYSTEM",
    "TYPE_CAST_FAILED",
    "INVALID_EMAIL",
    "INVALID_PHONE",
    "INVALID_CANONICAL_REF",
    "DUPLICATE_KEY_SUPERSEDED",
)


class SchemaContractError(Exception):
    """Batch-level contract violation: the whole run fails, nothing is written."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class RejectRateExceeded(Exception):
    """Too many rows rejected: the batch is treated as broken, not as data to keep."""


@dataclass(frozen=True)
class SchemaCheck:
    header: list[str]
    positions: dict[str, int]
    extra_columns: list[str]
    missing_optional: list[str]

    @property
    def warnings(self) -> list[str]:
        out = [f"extra column '{c}' dropped" for c in self.extra_columns]
        out += [f"optional column '{c}' absent, treated as NULL" for c in self.missing_optional]
        return out


def check_schema(header: list[str] | None) -> SchemaCheck:
    if not header:
        raise SchemaContractError("SCHEMA_MISSING_COLUMN", "file has no header row")
    names = [h.strip().lower() for h in header]
    seen: set[str] = set()
    duplicates = sorted({n for n in names if n in seen or seen.add(n)})
    if duplicates:
        raise SchemaContractError("SCHEMA_DUPLICATE_COLUMN", f"duplicate column(s) {duplicates}")
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise SchemaContractError("SCHEMA_MISSING_COLUMN", f"required column(s) {missing} absent")
    return SchemaCheck(
        header=header,
        positions={n: i for i, n in enumerate(names) if n in CONTRACT_COLUMNS},
        extra_columns=[n for n in names if n not in CONTRACT_COLUMNS],
        missing_optional=[c for c in OPTIONAL_COLUMNS if c not in names],
    )


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


# Original, untrimmed values of a t_raw row (aliased r). Built only for rejected rows.
RAW_PAYLOAD_SQL = "json_object(" + ", ".join(f"'{c}', r.{c}" for c in ("source_row_num",) + CONTRACT_COLUMNS) + ")"


def validate_rows(con: duckdb.DuckDBPyConnection, cfg: Config, run_id: str, source_file: str, file_sha: str) -> dict[str, int]:
    """t_raw + t_malformed -> quarantine_record (rejects) and t_valid (typed rows)."""
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE t_typed AS
            WITH typed AS (
                SELECT
                    r.source_row_num,
                    lower(idm_clean(r.source_system)) AS source_system,
                    idm_clean(r.source_id) AS source_id,
                    idm_clean(r.name) AS name,
                    idm_clean(r.email) AS email,
                    idm_clean(r.phone) AS phone,
                    idm_clean(r.domain) AS domain,
                    idm_clean(r.tax_id) AS tax_id,
                    idm_clean(r.canonical_ref) AS canonical_ref,
                    idm_clean(r.source_updated_at) AS source_updated_at_raw,
                    idm_parse_ts(r.source_updated_at) AS source_updated_at
                FROM t_raw r
            )
            SELECT t.*,
                CASE
                    WHEN t.source_system IS NULL OR t.source_id IS NULL OR t.source_updated_at_raw IS NULL
                        THEN 'MISSING_REQUIRED_FIELD'
                    WHEN t.source_system NOT IN ({_sql_list(cfg.allowed_source_systems)})
                        THEN 'UNKNOWN_SOURCE_SYSTEM'
                    WHEN t.source_updated_at IS NULL THEN 'TYPE_CAST_FAILED'
                    WHEN NOT idm_email_valid(t.email) THEN 'INVALID_EMAIL'
                    WHEN NOT idm_phone_valid(t.phone) THEN 'INVALID_PHONE'
                    WHEN t.canonical_ref IS NOT NULL AND c.canonical_id IS NULL THEN 'INVALID_CANONICAL_REF'
                END AS reject_reason_code
            FROM typed t
            LEFT JOIN canonical_entity c ON c.canonical_id = t.canonical_ref"""
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_valid AS
           SELECT * EXCLUDE (reject_reason_code, source_updated_at_raw) FROM t_typed
           WHERE reject_reason_code IS NULL"""
    )
    params = [run_id, source_file, file_sha]
    con.execute(
        """INSERT INTO quarantine_record
           SELECT sha256(?3 || ':' || source_row_num), ?1, ?2, source_row_num, raw_payload::JSON,
                  'MALFORMED_ROW', reject_detail, current_timestamp::TIMESTAMP
           FROM t_malformed""",
        params,
    )
    con.execute(
        f"""INSERT INTO quarantine_record
           SELECT sha256(?3 || ':' || t.source_row_num), ?1, ?2, t.source_row_num, {RAW_PAYLOAD_SQL},
                  t.reject_reason_code,
                  CASE t.reject_reason_code
                      WHEN 'MISSING_REQUIRED_FIELD' THEN 'null: ' || concat_ws(', ',
                          CASE WHEN t.source_system IS NULL THEN 'source_system' END,
                          CASE WHEN t.source_id IS NULL THEN 'source_id' END,
                          CASE WHEN t.source_updated_at_raw IS NULL THEN 'source_updated_at' END)
                      WHEN 'UNKNOWN_SOURCE_SYSTEM' THEN 'source_system=' || t.source_system
                      WHEN 'TYPE_CAST_FAILED' THEN 'source_updated_at=' || t.source_updated_at_raw
                      WHEN 'INVALID_EMAIL' THEN 'email=' || t.email
                      WHEN 'INVALID_PHONE' THEN 'phone=' || t.phone
                      WHEN 'INVALID_CANONICAL_REF' THEN 'canonical_ref=' || t.canonical_ref
                  END,
                  current_timestamp::TIMESTAMP
           FROM t_typed t JOIN t_raw r ON r.source_row_num = t.source_row_num
           WHERE t.reject_reason_code IS NOT NULL""",
        params,
    )
    counts = con.execute(
        """SELECT (SELECT count(*) FROM t_malformed),
                  (SELECT count(*) FROM t_typed WHERE reject_reason_code IS NOT NULL),
                  (SELECT count(*) FROM t_valid)"""
    ).fetchone()
    return {"malformed": counts[0], "invalid": counts[1], "valid": counts[2]}


def check_reject_rate(cfg: Config, rows_read: int, rejected: int) -> None:
    if rows_read >= max(cfg.reject_rate_min_rows, 1) and rejected / rows_read > cfg.max_reject_rate:
        raise RejectRateExceeded(
            f"{rejected}/{rows_read} rows rejected ({rejected / rows_read:.1%}) exceeds "
            f"validation.max_reject_rate={cfg.max_reject_rate:.0%}"
        )


def deduplicate(con: duckdb.DuckDBPyConnection, run_id: str, source_file: str, file_sha: str) -> dict[str, int]:
    """t_valid -> t_dedup (one row per key); superseded versions go to quarantine."""
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_ranked AS
           SELECT *,
                  row_number() OVER (
                      PARTITION BY source_system, source_id, name, email, phone, domain, tax_id,
                                   canonical_ref, source_updated_at
                      ORDER BY source_row_num) AS exact_rank
           FROM t_valid"""
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_versions AS
           SELECT * EXCLUDE (exact_rank),
                  row_number() OVER (
                      PARTITION BY source_system, source_id
                      ORDER BY source_updated_at DESC, source_row_num DESC) AS version_rank,
                  first_value(source_row_num) OVER (
                      PARTITION BY source_system, source_id
                      ORDER BY source_updated_at DESC, source_row_num DESC) AS winner_row_num,
                  first_value(source_updated_at) OVER (
                      PARTITION BY source_system, source_id
                      ORDER BY source_updated_at DESC, source_row_num DESC) AS winner_updated_at
           FROM t_ranked WHERE exact_rank = 1"""
    )
    con.execute(
        f"""INSERT INTO quarantine_record
           SELECT sha256(?3 || ':' || v.source_row_num), ?1, ?2, v.source_row_num, {RAW_PAYLOAD_SQL},
                  'DUPLICATE_KEY_SUPERSEDED',
                  'superseded by row ' || v.winner_row_num
                      || CASE WHEN v.winner_updated_at = v.source_updated_at
                              THEN ' (same source_updated_at; later row wins)'
                              ELSE ' (newer source_updated_at '
                                   || strftime(v.winner_updated_at, '%Y-%m-%dT%H:%M:%SZ') || ')' END,
                  current_timestamp::TIMESTAMP
           FROM t_versions v JOIN t_raw r ON r.source_row_num = v.source_row_num
           WHERE v.version_rank > 1""",
        [run_id, source_file, file_sha],
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_dedup AS
           SELECT * EXCLUDE (version_rank, winner_row_num, winner_updated_at)
           FROM t_versions WHERE version_rank = 1"""
    )
    counts = con.execute(
        """SELECT (SELECT count(*) FROM t_ranked WHERE exact_rank > 1),
                  (SELECT count(*) FROM t_versions WHERE version_rank > 1),
                  (SELECT count(*) FROM t_dedup)"""
    ).fetchone()
    con.execute("DROP TABLE t_ranked")
    con.execute("DROP TABLE t_versions")
    return {"exact_duplicates": counts[0], "superseded": counts[1], "unique_keys": counts[2]}
