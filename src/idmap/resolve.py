"""Resolution: turn rule hits into one decision per source key, then record the
outcome tables (conflicts, unresolved backlog, per-key status).

Decision, per key, from the highest-priority rule with any active hit:
    one distinct canonical   -> MAPPED
    several canonicals       -> CONFLICT (ONE_TO_MANY), never auto-picked
    no active hit            -> UNRESOLVED with reason
        INACTIVE_CANONICAL_ONLY   only inactive canonicals matched
        INSUFFICIENT_ATTRIBUTES   tax_id, email and name are all null
        NO_CANDIDATE              otherwise

Outcomes are applied only for records that are not late arrivals (see
publish.py). A key whose new outcome is CONFLICT or UNRESOLVED keeps its
current mapping, and the outcome row is flagged `has_existing_mapping`.
"""

from __future__ import annotations

import duckdb

ONE_TO_MANY = "ONE_TO_MANY"
MANY_TO_ONE = "MANY_TO_ONE_SAME_SYSTEM"


def build_decisions(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """t_stage + t_hits -> t_decision (one row per key)."""
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_decision AS
           WITH active AS (
               SELECT * FROM t_hits WHERE is_active
           ),
           best AS (
               SELECT source_system, source_id, min(rule_priority) AS best_priority
               FROM active GROUP BY source_system, source_id
           ),
           top AS (
               SELECT a.source_system, a.source_id,
                      list_sort(list_distinct(list(a.canonical_id))) AS candidate_ids,
                      array_to_string(list_sort(list_distinct(list(a.rule_id))), '+') AS rule_id
               FROM active a
               JOIN best b ON a.source_system = b.source_system AND a.source_id = b.source_id
                          AND a.rule_priority = b.best_priority
               GROUP BY a.source_system, a.source_id
           ),
           inactive AS (
               SELECT DISTINCT source_system, source_id FROM t_hits WHERE NOT is_active
           )
           SELECT s.source_system, s.source_id, s.source_updated_at,
                  CASE WHEN t.candidate_ids IS NULL THEN 'UNRESOLVED'
                       WHEN len(t.candidate_ids) = 1 THEN 'MAPPED'
                       ELSE 'CONFLICT' END AS decision,
                  CASE WHEN len(t.candidate_ids) = 1 THEN t.candidate_ids[1] END AS canonical_id,
                  t.rule_id,
                  t.candidate_ids,
                  CASE WHEN t.candidate_ids IS NOT NULL THEN NULL
                       WHEN i.source_id IS NOT NULL THEN 'INACTIVE_CANONICAL_ONLY'
                       WHEN s.tax_id_norm IS NULL AND s.email_norm IS NULL AND s.name_norm IS NULL
                           THEN 'INSUFFICIENT_ATTRIBUTES'
                       ELSE 'NO_CANDIDATE' END AS unresolved_reason
           FROM t_stage s
           LEFT JOIN top t ON t.source_system = s.source_system AND t.source_id = s.source_id
           LEFT JOIN inactive i ON i.source_system = s.source_system AND i.source_id = s.source_id"""
    )
    rows = con.execute("SELECT decision, count(*) FROM t_decision GROUP BY decision").fetchall()
    counts = {"MAPPED": 0, "CONFLICT": 0, "UNRESOLVED": 0}
    counts.update(dict(rows))
    return counts


def record_outcomes(con: duckdb.DuckDBPyConnection, run_id: str, file_sha: str) -> dict[str, int]:
    """Apply fresh (non-late) decisions in t_eval to the outcome tables."""
    params = {"run": run_id}

    # ONE_TO_MANY conflicts: open or refresh, then resolve keys that no longer conflict.
    con.execute(
        f"""INSERT INTO mapping_conflict
            SELECT sha256('{ONE_TO_MANY}|' || source_system || '|' || source_id), '{ONE_TO_MANY}',
                   source_system, source_id, candidate_ids, rule_id,
                   cur_canonical_id IS NOT NULL, cur_canonical_id, 'OPEN', $run, $run, NULL
            FROM t_eval WHERE NOT is_stale AND decision = 'CONFLICT'
            ON CONFLICT (conflict_id) DO UPDATE SET
                candidate_canonical_ids = excluded.candidate_canonical_ids,
                rule_id = excluded.rule_id,
                has_existing_mapping = excluded.has_existing_mapping,
                existing_canonical_id = excluded.existing_canonical_id,
                status = 'OPEN',
                last_detected_run_id = excluded.last_detected_run_id,
                resolved_run_id = NULL""",
        params,
    )
    con.execute(
        f"""UPDATE mapping_conflict SET status = 'RESOLVED', resolved_run_id = $run
            FROM t_eval e
            WHERE mapping_conflict.conflict_type = '{ONE_TO_MANY}' AND mapping_conflict.status = 'OPEN'
              AND mapping_conflict.source_system = e.source_system AND mapping_conflict.source_id = e.source_id
              AND NOT e.is_stale AND e.decision <> 'CONFLICT'""",
        params,
    )

    # Unresolved backlog. Reprocessing the same file is not a new attempt.
    con.execute(
        """INSERT INTO unresolved_mapping
           SELECT e.source_system, e.source_id, e.unresolved_reason, e.cur_canonical_id IS NOT NULL, 'OPEN',
                  $run, $run, NULL,
                  CASE WHEN u.source_id IS NULL THEN 1
                       WHEN u.status = 'OPEN' AND pr.file_sha256 = $sha THEN u.attempt_count
                       ELSE u.attempt_count + 1 END
           FROM t_eval e
           LEFT JOIN unresolved_mapping u ON u.source_system = e.source_system AND u.source_id = e.source_id
           LEFT JOIN pipeline_run pr ON pr.run_id = u.last_seen_run_id
           WHERE NOT e.is_stale AND e.decision = 'UNRESOLVED'
           ON CONFLICT (source_system, source_id) DO UPDATE SET
               reason = excluded.reason,
               has_existing_mapping = excluded.has_existing_mapping,
               status = 'OPEN',
               last_seen_run_id = excluded.last_seen_run_id,
               resolved_run_id = NULL,
               attempt_count = excluded.attempt_count""",
        {**params, "sha": file_sha},
    )
    con.execute(
        """UPDATE unresolved_mapping SET status = 'RESOLVED', resolved_run_id = $run
           FROM t_eval e
           WHERE unresolved_mapping.status = 'OPEN'
             AND unresolved_mapping.source_system = e.source_system AND unresolved_mapping.source_id = e.source_id
             AND NOT e.is_stale AND e.decision <> 'UNRESOLVED'""",
        params,
    )

    # Per-key status: the reconciliation anchor. canonical_id is the current mapping
    # after this run, which a CONFLICT or UNRESOLVED key may still hold.
    con.execute(
        f"""INSERT INTO source_key_status
            SELECT source_system, source_id, decision,
                   CASE decision WHEN 'MAPPED' THEN rule_id
                                 WHEN 'CONFLICT' THEN '{ONE_TO_MANY}'
                                 ELSE unresolved_reason END,
                   CASE WHEN decision = 'MAPPED' THEN canonical_id ELSE cur_canonical_id END,
                   $run, source_updated_at
            FROM t_eval WHERE NOT is_stale
            ON CONFLICT (source_system, source_id) DO UPDATE SET
                status = excluded.status,
                status_reason = excluded.status_reason,
                canonical_id = excluded.canonical_id,
                last_run_id = excluded.last_run_id,
                last_source_updated_at = excluded.last_source_updated_at""",
        params,
    )

    # MANY_TO_ONE_SAME_SYSTEM: a warning over all current mappings; the mappings stay.
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_many_to_one AS
           SELECT source_system, source_id, canonical_id, match_rule
           FROM id_mapping_history WHERE is_current
           QUALIFY count(*) OVER (PARTITION BY source_system, canonical_id) > 1"""
    )
    con.execute(
        f"""INSERT INTO mapping_conflict
            SELECT sha256('{MANY_TO_ONE}|' || source_system || '|' || source_id), '{MANY_TO_ONE}',
                   source_system, source_id, [canonical_id], match_rule, true, canonical_id, 'OPEN', $run, $run, NULL
            FROM t_many_to_one
            ON CONFLICT (conflict_id) DO UPDATE SET
                candidate_canonical_ids = excluded.candidate_canonical_ids,
                rule_id = excluded.rule_id,
                existing_canonical_id = excluded.existing_canonical_id,
                status = 'OPEN',
                last_detected_run_id = excluded.last_detected_run_id,
                resolved_run_id = NULL""",
        params,
    )
    con.execute(
        f"""UPDATE mapping_conflict SET status = 'RESOLVED', resolved_run_id = $run
            WHERE conflict_type = '{MANY_TO_ONE}' AND status = 'OPEN'
              AND NOT EXISTS (SELECT 1 FROM t_many_to_one m
                              WHERE m.source_system = mapping_conflict.source_system
                                AND m.source_id = mapping_conflict.source_id)""",
        params,
    )
    many_to_one_open = con.execute(
        f"SELECT count(*) FROM mapping_conflict WHERE conflict_type = '{MANY_TO_ONE}' AND status = 'OPEN'"
    ).fetchone()[0]
    con.execute("DROP TABLE t_many_to_one")
    return {"many_to_one_open": many_to_one_open}
