"""Row-by-row Python baseline: normalize, match and resolve one record at a time.

This is the "before" in scripts/perf.py. Normalization uses the Python
functions in normalize.py, canonical keys go into dicts, and each source record
does its own dict lookups. It implements the same rules as the set-based SQL
path. Tests and the perf script assert that both engines make identical decisions.
"""

from __future__ import annotations

from collections.abc import Iterable

from idmap.config import Config
from idmap.normalize import PyNormalizer

Decision = tuple[str, str | None, str | None, tuple[str, ...] | None, str | None]


def _rule_keys(tax, email, name, domain, phone) -> dict[str, str]:
    keys = {}
    if tax is not None:
        keys["exact_tax_id"] = tax
    if email is not None:
        keys["exact_email"] = email
    if name is not None and domain is not None:
        keys["rule_name_domain"] = f"{name}|{domain}"
    if name is not None and phone is not None:
        keys["rule_phone_name"] = f"{phone}|{name.split(' ')[0]}"
    return keys


class BaselineMatcher:
    def __init__(self, cfg: Config, canonicals: Iterable[tuple], overrides: Iterable[tuple]):
        """canonicals: (canonical_id, legal_name, tax_id, primary_email, domain, phone, is_active);
        overrides: (source_system, source_id, canonical_id)."""
        self.norm = PyNormalizer(cfg)
        self.rules = list(cfg.enabled_rules)
        self.active: dict[str, bool] = {}
        self.index: dict[str, dict[str, list[str]]] = {r.id: {} for r in self.rules}
        for cid, legal_name, tax, email, domain, phone, is_active in canonicals:
            self.active[cid] = bool(is_active)
            t, e, n, d, p = self.norm.row(legal_name, email, phone, domain, tax)
            for rule_id, key in _rule_keys(t, e, n, d, p).items():
                if rule_id in self.index:
                    self.index[rule_id].setdefault(key, []).append(cid)
        self.overrides = {(s, i): c for s, i, c in overrides}
        # Rules sharing a priority pool their hits, exactly as in the SQL resolution.
        self.priority_groups: list[list[str]] = []
        last_priority = None
        for rule in self.rules:  # cfg.rules is sorted by priority
            if rule.priority == last_priority:
                self.priority_groups[-1].append(rule.id)
            else:
                self.priority_groups.append([rule.id])
                last_priority = rule.priority

    def decide(self, system: str, source_id: str, name, email, phone, domain, tax) -> Decision:
        t, e, n, d, p = self.norm.row(name, email, phone, domain, tax)
        keys = _rule_keys(t, e, n, d, p)
        saw_inactive = False
        for group in self.priority_groups:
            hits: list[tuple[str, str]] = []
            for rule_id in group:
                if rule_id == "manual_override":
                    cid = self.overrides.get((system, source_id))
                    if cid is not None:
                        hits.append((rule_id, cid))
                elif rule_id in keys:
                    hits.extend((rule_id, cid) for cid in self.index[rule_id].get(keys[rule_id], ()))
            live = [(r, c) for r, c in hits if self.active.get(c)]
            saw_inactive = saw_inactive or len(live) < len(hits)
            if live:
                ids = tuple(sorted({c for _, c in live}))
                rule = "+".join(sorted({r for r, _ in live}))
                if len(ids) == 1:
                    return ("MAPPED", ids[0], rule, ids, None)
                return ("CONFLICT", None, rule, ids, None)
        if saw_inactive:
            return ("UNRESOLVED", None, None, None, "INACTIVE_CANONICAL_ONLY")
        if t is None and e is None and n is None:
            return ("UNRESOLVED", None, None, None, "INSUFFICIENT_ATTRIBUTES")
        return ("UNRESOLVED", None, None, None, "NO_CANDIDATE")


def baseline_decisions(cfg: Config, canonicals, overrides, rows: Iterable[tuple]) -> dict[tuple[str, str], Decision]:
    """rows: (source_system, source_id, name, email, phone, domain, tax_id)."""
    matcher = BaselineMatcher(cfg, canonicals, overrides)
    return {(r[0], r[1]): matcher.decide(*r) for r in rows}
