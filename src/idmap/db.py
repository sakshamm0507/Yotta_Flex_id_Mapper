"""DuckDB connection, schema (DDL) and the transaction helper.

Primary keys are declared on reference and state tables, the ones that are
upserted. The append-only batch tables (staging, quarantine, candidates,
late arrivals) have logical keys that hold by construction, and the
reconciliation report checks them. Foreign keys are not declared: DuckDB
blocks updates to a parent row that has children, which would rule out
refreshing the canonical master. Referential integrity is a report check instead.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

from idmap.config import Config
from idmap.normalize import register_macros

SCHEMA_VERSION = 1

DDL = [
    """CREATE TABLE IF NOT EXISTS schema_info (
        schema_version INTEGER NOT NULL
    )""",
    # Reference data -----------------------------------------------------------
    """CREATE TABLE IF NOT EXISTS canonical_entity (
        canonical_id   VARCHAR PRIMARY KEY,
        legal_name     VARCHAR NOT NULL,
        tax_id         VARCHAR,
        primary_email  VARCHAR,
        domain         VARCHAR,
        phone          VARCHAR,
        is_active      BOOLEAN NOT NULL,
        created_at     TIMESTAMP
    )""",
    # One row per (rule, normalized key, canonical): the blocking index the rules join on.
    """CREATE TABLE IF NOT EXISTS canonical_key (
        rule_id        VARCHAR NOT NULL,
        key_value      VARCHAR NOT NULL,
        canonical_id   VARCHAR NOT NULL,
        is_active      BOOLEAN NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS mapping_override (
        source_system  VARCHAR NOT NULL,
        source_id      VARCHAR NOT NULL,
        canonical_id   VARCHAR NOT NULL,
        reason         VARCHAR,
        created_at     TIMESTAMP,
        PRIMARY KEY (source_system, source_id)
    )""",
    """CREATE TABLE IF NOT EXISTS reference_load (
        reference_name VARCHAR PRIMARY KEY,
        source_file    VARCHAR NOT NULL,
        file_sha256    VARCHAR NOT NULL,
        row_count      BIGINT NOT NULL,
        loaded_at      TIMESTAMP NOT NULL
    )""",
    # Operational / batch tables -----------------------------------------------
    """CREATE TABLE IF NOT EXISTS pipeline_run (
        run_id              VARCHAR PRIMARY KEY,
        run_seq             BIGINT NOT NULL,
        source_file         VARCHAR NOT NULL,
        file_sha256         VARCHAR,
        reference_sha256    VARCHAR,
        forced              BOOLEAN NOT NULL DEFAULT false,
        status              VARCHAR NOT NULL,
        started_at          TIMESTAMP NOT NULL,
        ended_at            TIMESTAMP,
        duration_ms         BIGINT,
        rows_read           BIGINT,
        rows_staged         BIGINT,
        rows_quarantined    BIGINT,
        duplicates_dropped  BIGINT,
        mapped              BIGINT,
        conflicts           BIGINT,
        unresolved          BIGINT,
        mappings_inserted   BIGINT,
        mappings_closed     BIGINT,
        late_arrivals       BIGINT,
        schema_warnings     BIGINT,
        many_to_one_open    BIGINT,
        failed_stage        VARCHAR,
        failure_reason      VARCHAR,
        status_detail       VARCHAR
    )""",
    """CREATE TABLE IF NOT EXISTS processed_file (
        file_sha256    VARCHAR PRIMARY KEY,
        source_file    VARCHAR NOT NULL,
        run_id         VARCHAR NOT NULL,
        status         VARCHAR NOT NULL,
        processed_at   TIMESTAMP NOT NULL
    )""",
    # Logical key: (run_id, source_system, source_id).
    """CREATE TABLE IF NOT EXISTS stg_source_record (
        run_id             VARCHAR NOT NULL,
        source_file        VARCHAR NOT NULL,
        source_row_num     BIGINT NOT NULL,
        source_system      VARCHAR NOT NULL,
        source_id          VARCHAR NOT NULL,
        name               VARCHAR,
        email              VARCHAR,
        phone              VARCHAR,
        domain             VARCHAR,
        tax_id             VARCHAR,
        canonical_ref      VARCHAR,
        source_updated_at  TIMESTAMP NOT NULL,
        tax_id_norm        VARCHAR,
        email_norm         VARCHAR,
        phone_norm         VARCHAR,
        name_norm          VARCHAR,
        domain_norm        VARCHAR,
        ingested_at        TIMESTAMP NOT NULL
    )""",
    # Logical key: quarantine_id = sha256(file_sha256:source_row_num).
    """CREATE TABLE IF NOT EXISTS quarantine_record (
        quarantine_id       VARCHAR NOT NULL,
        run_id              VARCHAR NOT NULL,
        source_file         VARCHAR NOT NULL,
        source_row_num      BIGINT NOT NULL,
        raw_payload         JSON,
        reject_reason_code  VARCHAR NOT NULL,
        reject_detail       VARCHAR,
        quarantined_at      TIMESTAMP NOT NULL
    )""",
    # Logical key: (run_id, source_system, source_id, rule_id, canonical_id).
    """CREATE TABLE IF NOT EXISTS match_candidate (
        run_id          VARCHAR NOT NULL,
        source_system   VARCHAR NOT NULL,
        source_id       VARCHAR NOT NULL,
        canonical_id    VARCHAR NOT NULL,
        rule_id         VARCHAR NOT NULL,
        rule_priority   INTEGER NOT NULL,
        matched_value   VARCHAR
    )""",
    # State tables -------------------------------------------------------------
    """CREATE TABLE IF NOT EXISTS id_mapping_history (
        mapping_id      VARCHAR PRIMARY KEY,
        source_system   VARCHAR NOT NULL,
        source_id       VARCHAR NOT NULL,
        canonical_id    VARCHAR NOT NULL,
        match_rule      VARCHAR NOT NULL,
        valid_from      TIMESTAMP NOT NULL,
        valid_to        TIMESTAMP,
        is_current      BOOLEAN NOT NULL,
        created_run_id  VARCHAR NOT NULL,
        closed_run_id   VARCHAR,
        recorded_at     TIMESTAMP NOT NULL
    )""",
    """CREATE OR REPLACE VIEW id_mapping_current AS
        SELECT * FROM id_mapping_history WHERE is_current""",
    """CREATE TABLE IF NOT EXISTS mapping_conflict (
        conflict_id              VARCHAR PRIMARY KEY,
        conflict_type            VARCHAR NOT NULL,
        source_system            VARCHAR NOT NULL,
        source_id                VARCHAR NOT NULL,
        candidate_canonical_ids  VARCHAR[] NOT NULL,
        rule_id                  VARCHAR NOT NULL,
        has_existing_mapping     BOOLEAN NOT NULL,
        existing_canonical_id    VARCHAR,
        status                   VARCHAR NOT NULL,
        first_detected_run_id    VARCHAR NOT NULL,
        last_detected_run_id     VARCHAR NOT NULL,
        resolved_run_id          VARCHAR
    )""",
    """CREATE TABLE IF NOT EXISTS unresolved_mapping (
        source_system         VARCHAR NOT NULL,
        source_id             VARCHAR NOT NULL,
        reason                VARCHAR NOT NULL,
        has_existing_mapping  BOOLEAN NOT NULL,
        status                VARCHAR NOT NULL,
        first_seen_run_id     VARCHAR NOT NULL,
        last_seen_run_id      VARCHAR NOT NULL,
        resolved_run_id       VARCHAR,
        attempt_count         INTEGER NOT NULL,
        PRIMARY KEY (source_system, source_id)
    )""",
    """CREATE TABLE IF NOT EXISTS source_key_status (
        source_system           VARCHAR NOT NULL,
        source_id               VARCHAR NOT NULL,
        status                  VARCHAR NOT NULL,
        status_reason           VARCHAR NOT NULL,
        canonical_id            VARCHAR,
        last_run_id             VARCHAR NOT NULL,
        last_source_updated_at  TIMESTAMP NOT NULL,
        PRIMARY KEY (source_system, source_id)
    )""",
    # Logical key: (run_id, source_system, source_id).
    """CREATE TABLE IF NOT EXISTS late_arrival_log (
        run_id                     VARCHAR NOT NULL,
        source_system              VARCHAR NOT NULL,
        source_id                  VARCHAR NOT NULL,
        record_updated_at          TIMESTAMP NOT NULL,
        latest_applied_updated_at  TIMESTAMP NOT NULL,
        current_valid_from         TIMESTAMP,
        current_canonical_id       VARCHAR,
        proposed_status            VARCHAR NOT NULL,
        proposed_canonical_id      VARCHAR,
        action                     VARCHAR NOT NULL
    )""",
]

# Tables whose rows belong to one batch file. A forced rerun replaces them per checksum.
BATCH_TABLES = ("stg_source_record", "quarantine_record", "match_candidate", "late_arrival_log")
STATE_TABLES = ("id_mapping_history", "mapping_conflict", "unresolved_mapping", "source_key_status")


def connect(cfg: Config) -> duckdb.DuckDBPyConnection:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.db_path))
    con.execute("SET TimeZone = 'UTC'")
    if cfg.duckdb_threads:
        con.execute(f"SET threads = {int(cfg.duckdb_threads)}")
    if cfg.duckdb_memory_limit:
        con.execute(f"SET memory_limit = '{cfg.duckdb_memory_limit}'")
    ensure_schema(con)
    register_macros(con, cfg)
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for statement in DDL:
        con.execute(statement)
    if con.execute("SELECT count(*) FROM schema_info").fetchone()[0] == 0:
        con.execute("INSERT INTO schema_info VALUES (?)", [SCHEMA_VERSION])


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """BEGIN ... COMMIT, rolling back on any exception (including a failed COMMIT)."""
    con.execute("BEGIN TRANSACTION")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass  # the failed COMMIT already aborted the transaction
        raise


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    row = con.execute(sql, params or []).fetchone()
    return None if row is None else row[0]
