"""SCD Type 2 publish into id_mapping_history, with late-arrival protection.

A record is a late arrival when its source_updated_at is older than the newest
evidence already applied to its key (source_key_status.last_source_updated_at).
That bound is never earlier than the current mapping's valid_from. A late
arrival changes nothing: it is logged to late_arrival_log with the outcome it
would have produced, and current state stays as it is.

For records that are not late (MAPPED decisions only):
    no current mapping        -> insert a current row
    same canonical as current -> no-op
    different canonical       -> close current (valid_to = new valid_from), insert new

valid_from is the record's event time, not the run time, and mapping_id hashes
(source_system, source_id, canonical_id, valid_from). Rebuilding from the same
inputs therefore reproduces the same history rows.
"""

from __future__ import annotations

import duckdb

from idmap.metrics import utc_now

MAPPING_ID_SQL = (
    "sha256(source_system || '|' || source_id || '|' || canonical_id || '|' "
    "|| strftime(source_updated_at, '%Y-%m-%dT%H:%M:%S.%f'))"
)


def build_eval(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """t_decision + current state -> t_eval, flagging late arrivals."""
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_eval AS
           SELECT d.*,
                  k.last_source_updated_at AS latest_applied_updated_at,
                  m.mapping_id AS cur_mapping_id,
                  m.canonical_id AS cur_canonical_id,
                  m.valid_from AS cur_valid_from,
                  coalesce(d.source_updated_at < k.last_source_updated_at, false) AS is_stale
           FROM t_decision d
           LEFT JOIN source_key_status k
                  ON k.source_system = d.source_system AND k.source_id = d.source_id
           LEFT JOIN id_mapping_history m
                  ON m.is_current AND m.source_system = d.source_system AND m.source_id = d.source_id"""
    )
    row = con.execute(
        """SELECT count(*) FILTER (WHERE is_stale),
                  count(*) FILTER (WHERE NOT is_stale AND decision = 'MAPPED'),
                  count(*) FILTER (WHERE NOT is_stale AND decision = 'CONFLICT'),
                  count(*) FILTER (WHERE NOT is_stale AND decision = 'UNRESOLVED')
           FROM t_eval"""
    ).fetchone()
    return {"late_arrivals": row[0], "mapped": row[1], "conflicts": row[2], "unresolved": row[3]}


def publish(con: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, int]:
    con.execute(
        """INSERT INTO late_arrival_log
           SELECT ?, source_system, source_id, source_updated_at, latest_applied_updated_at,
                  cur_valid_from, cur_canonical_id, decision, canonical_id, 'IGNORED_FOR_CURRENT'
           FROM t_eval WHERE is_stale""",
        [run_id],
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_changes AS
           SELECT * FROM t_eval
           WHERE NOT is_stale AND decision = 'MAPPED'
             AND (cur_canonical_id IS NULL OR cur_canonical_id <> canonical_id)"""
    )
    closed = con.execute(
        "SELECT count(*) FROM t_changes WHERE cur_mapping_id IS NOT NULL"
    ).fetchone()[0]
    con.execute(
        """UPDATE id_mapping_history
           SET valid_to = c.source_updated_at, is_current = false, closed_run_id = ?
           FROM t_changes c
           WHERE id_mapping_history.mapping_id = c.cur_mapping_id"""
        ,
        [run_id],
    )
    # A mapping_id can already exist when two inputs carry the same event time for
    # different canonicals and are applied alternately. Reopen that row rather
    # than duplicate it.
    con.execute(
        f"""INSERT INTO id_mapping_history
            SELECT {MAPPING_ID_SQL}, source_system, source_id, canonical_id, rule_id,
                   source_updated_at, NULL, true, ?, NULL, ?
            FROM t_changes
            ON CONFLICT (mapping_id) DO UPDATE SET
                valid_to = NULL, is_current = true, closed_run_id = NULL""",
        [run_id, utc_now()],
    )
    inserted = con.execute("SELECT count(*) FROM t_changes").fetchone()[0]
    con.execute("DROP TABLE t_changes")
    return {"mappings_inserted": inserted, "mappings_closed": closed}
