"""The row-by-row Python baseline and the set-based SQL engine must agree exactly."""

from __future__ import annotations

import unicodedata

import pytest

from conftest import row
from idmap import db, match, normalize, resolve
from idmap.baseline import baseline_decisions
from idmap.config import Rule

ROWS = [
    row("C1", tax_id="tx 100-0001", name="Stark Industries", domain="stark.example"),
    row("C2", email=" HELLO@initech.example "),
    row("C3", name="Initech, Inc.", domain="www.initech.example/"),
    row("C4", tax_id="TX-100-0005"),
    row("C5", tax_id="TX-100-0005", email="info@globex.example"),
    row("C6", name="Wayne Ent", phone="(415) 555-0107"),
    row("C7", phone="415 555 0107", domain="wayne.example"),
    row("C8", name=unicodedata.normalize("NFD", "SOCIÉTÉ GÉNÉRALE DES EAUX"), domain="sge.example"),
    row("C9", name="Unknown Org", email="x@unknown.example"),
    row("C10", name="   ", email="\t"),
    row("C11", tax_id="TX-100-0001", email="info@globex.example"),
]


def _compare(ws) -> None:
    ws.run_one(ws.batch("crm_batch_001.csv", ROWS))
    con = db.connect(ws.cfg)
    try:
        con.execute("""CREATE TEMP TABLE t_dedup AS
                       SELECT source_system, source_id, name, email, phone, domain, tax_id, canonical_ref,
                              source_updated_at, source_row_num FROM stg_source_record""")
        normalize.normalize_batch(con)
        match.build_hits(con, ws.cfg)
        resolve.build_decisions(con)
        sql = {(r[0], r[1]): (r[2], r[3], r[4], tuple(r[5]) if r[5] else None, r[6]) for r in con.execute(
            """SELECT source_system, source_id, decision, canonical_id, rule_id, candidate_ids, unresolved_reason
               FROM t_decision""").fetchall()}
        py = baseline_decisions(
            ws.cfg,
            con.execute("SELECT canonical_id, legal_name, tax_id, primary_email, domain, phone, is_active FROM canonical_entity").fetchall(),
            con.execute("SELECT source_system, source_id, canonical_id FROM mapping_override").fetchall(),
            con.execute("SELECT source_system, source_id, name, email, phone, domain, tax_id FROM stg_source_record").fetchall(),
        )
    finally:
        con.close()
    assert py == sql
    assert {d[0] for d in sql.values()} == {"MAPPED", "CONFLICT", "UNRESOLVED"}


def test_engines_agree_default_rules(ws):
    _compare(ws)


@pytest.mark.parametrize("rules", [
    (Rule("manual_override", 0), Rule("exact_tax_id", 1), Rule("exact_email", 1), Rule("rule_name_domain", 2),
     Rule("rule_phone_name", 3)),
    (Rule("manual_override", 0), Rule("exact_tax_id", 4), Rule("exact_email", 3, enabled=False),
     Rule("rule_name_domain", 2), Rule("rule_phone_name", 1)),
])
def test_engines_agree_with_custom_rule_config(ws, rules):
    ws.write_overrides([("crm", "C9", "can_000007")])
    ws.cfg = ws.cfg.replace(rules=tuple(sorted(rules, key=lambda r: (r.priority, r.id))))
    _compare(ws)
