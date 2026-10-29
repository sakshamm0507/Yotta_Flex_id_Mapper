"""Rule engine: candidate generation through equi-joins on normalized blocking keys.

| Priority | Rule             | Key                                         |
|----------|------------------|---------------------------------------------|
| P0       | manual_override  | (source_system, source_id) in the crosswalk |
| P1       | exact_tax_id     | tax_id_norm                                 |
| P2       | exact_email      | email_norm = canonical primary_email        |
| P3       | rule_name_domain | name_norm + domain_norm                     |
| P4       | rule_phone_name  | phone_norm + first token of name_norm       |

Rules P1 to P4 produce one (rule_id, key_value) pair per source row. Canonicals
get the same pairs in `canonical_key`, so matching is one hash join with no
cross joins and no pairwise scoring. Hits against inactive canonicals are kept in
`t_hits` so resolution can report INACTIVE_CANONICAL_ONLY, but only active hits
are candidates (`match_candidate`).
"""

from __future__ import annotations

import duckdb

from idmap.config import Config

# rule_id -> (key expression, presence condition) over the normalized columns.
RULE_KEYS: dict[str, tuple[str, str]] = {
    "exact_tax_id": ("tax_id_norm", "tax_id_norm IS NOT NULL"),
    "exact_email": ("email_norm", "email_norm IS NOT NULL"),
    "rule_name_domain": ("name_norm || '|' || domain_norm", "name_norm IS NOT NULL AND domain_norm IS NOT NULL"),
    "rule_phone_name": (
        "phone_norm || '|' || split_part(name_norm, ' ', 1)",
        "phone_norm IS NOT NULL AND name_norm IS NOT NULL",
    ),
}


def rule_keys_sql(table: str, id_columns: str, rule_ids: list[str] | None = None) -> str:
    """UNION ALL of (id_columns, rule_id, key_value) for each attribute rule."""
    parts = []
    for rule_id, (expr, condition) in RULE_KEYS.items():
        if rule_ids is None or rule_id in rule_ids:
            parts.append(
                f"SELECT {id_columns}, '{rule_id}' AS rule_id, {expr} AS key_value FROM {table} WHERE {condition}"
            )
    return "\nUNION ALL\n".join(parts)


def build_hits(con: duckdb.DuckDBPyConnection, cfg: Config) -> int:
    """t_stage -> t_hits: every rule hit, active and inactive canonicals alike."""
    attribute_rules = [r for r in cfg.enabled_rules if r.id in RULE_KEYS]
    selects = []
    override_priority = cfg.rule_priority("manual_override")
    if override_priority is not None:
        selects.append(
            f"""SELECT s.source_system, s.source_id, o.canonical_id, 'manual_override' AS rule_id,
                       {override_priority} AS rule_priority,
                       s.source_system || ':' || s.source_id AS matched_value, c.is_active
                FROM t_stage s
                JOIN mapping_override o ON o.source_system = s.source_system AND o.source_id = s.source_id
                JOIN canonical_entity c ON c.canonical_id = o.canonical_id"""
        )
    if attribute_rules:
        priority_case = " ".join(f"WHEN '{r.id}' THEN {r.priority}" for r in attribute_rules)
        keys = rule_keys_sql("t_stage", "source_system, source_id", [r.id for r in attribute_rules])
        selects.append(
            f"""SELECT k.source_system, k.source_id, ck.canonical_id, k.rule_id,
                       CASE k.rule_id {priority_case} END AS rule_priority,
                       k.key_value AS matched_value, ck.is_active
                FROM ({keys}) k
                JOIN canonical_key ck ON ck.rule_id = k.rule_id AND ck.key_value = k.key_value"""
        )
    if not selects:
        selects.append(
            "SELECT NULL::VARCHAR AS source_system, NULL::VARCHAR AS source_id, NULL::VARCHAR AS canonical_id, "
            "NULL::VARCHAR AS rule_id, NULL::INTEGER AS rule_priority, NULL::VARCHAR AS matched_value, "
            "NULL::BOOLEAN AS is_active WHERE false"
        )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE t_hits AS\n"
        + "\nUNION ALL\n".join(selects)
    )
    return con.execute("SELECT count(*) FROM t_hits").fetchone()[0]


def record_candidates(con: duckdb.DuckDBPyConnection, run_id: str) -> int:
    con.execute(
        """INSERT INTO match_candidate
           SELECT ?, source_system, source_id, canonical_id, rule_id, rule_priority, matched_value
           FROM t_hits WHERE is_active""",
        [run_id],
    )
    return con.execute("SELECT count(*) FROM t_hits WHERE is_active").fetchone()[0]
