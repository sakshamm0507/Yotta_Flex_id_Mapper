#!/usr/bin/env python3
"""Seeded synthetic data generator with ground-truth labels.

    python scripts/generate.py --n 100000 --seed 42 --out data/generated
    python scripts/generate.py --sample --seed 7 --out data/sample --expected-out data/expected

Writes to --out:
    canonical_entity.csv, mapping_override.csv           reference data
    billing_batch_001.csv, crm_batch_001.csv, support_batch_001.csv
    crm_batch_002.csv        late re-sends (older timestamps) plus remaps (newer)
    schema_drift_batch.csv   extra column, reordered columns, optional column absent
    broken_schema_batch.csv  required column absent (the run must FAIL)
and to --expected-out (default <out>/expected), kept apart from the inputs:
    expected_labels.csv      one row per business key: expected status / canonical / rule,
                             the true entity, and the scenario that produced it
    expected_quarantine.csv  file + physical line + reason code of every row that must be rejected
    expected_summary.json    designed volumes (duplicates, late arrivals, ...)

The generator does not import the pipeline. Every source record is built from a
known entity. Attribute noise (case, punctuation, phone and URL formats, legal
suffixes) is chosen so that it disappears under the documented normalization
rules. The expected outcome is then the documented rule order applied to the
designed key values, with key collisions known by construction. The pipeline
has to reach the same answer from the noisy text.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SYSTEMS = ("crm", "billing", "support")
ID_PREFIX = {"crm": "CRM", "billing": "BIL", "support": "SUP"}
CONTRACT = ("source_system", "source_id", "name", "email", "phone", "domain", "tax_id", "canonical_ref", "source_updated_at")
RULE_ORDER = ("manual_override", "exact_tax_id", "exact_email", "rule_name_domain", "rule_phone_name")
NAME_SUFFIX_TOKENS = ("inc", "incorporated", "ltd", "limited", "pvt", "private", "llc", "corp", "corporation")

# Probability that a source system carries each attribute for an entity it knows.
APPEAR = {"crm": 0.75, "billing": 0.65, "support": 0.60}
PRESENCE = {
    "crm": {"name": 0.98, "email": 0.85, "primary_email": 0.80, "phone": 0.60, "domain": 0.70, "tax": 0.35, "ref": 0.15},
    "billing": {"name": 0.95, "email": 0.70, "primary_email": 0.40, "phone": 0.50, "domain": 0.30, "tax": 0.90, "ref": 0.0},
    "support": {"name": 0.88, "email": 0.95, "primary_email": 0.60, "phone": 0.50, "domain": 0.40, "tax": 0.05, "ref": 0.0},
}

WORDS_A = ("Blue Silver Northern Southern Eastern Western Golden Granite Summit Pacific Atlantic Crimson Emerald "
           "Ivory Amber Cobalt Coral Maple Cedar Willow Aspen Birch Oak Pine Harbor River Lake Valley Canyon "
           "Prairie Meadow Highland Coastal Island Desert Arctic Polar Solar Lunar Stellar Quantum Vector Apex "
           "Pinnacle Vertex Zenith Horizon Frontier Pioneer Liberty Heritage Legacy Unity Royal Crown Noble Swift "
           "Bright Clear").split()
WORDS_B = ("Falcon Eagle Hawk Wolf Bear Fox Lion Tiger Otter Heron Raven Sparrow Bridge Gate Tower Forge Mill "
           "Point Ridge Peak Stone Rock Field Grove Wood Brook Spring Bay Port Crest Path Trail Way Line Link "
           "Wave Spark Flame Light Star Sky Cloud Rain Wind Frost Leaf Root Seed Bloom").split()
WORDS_C = ("Logistics Foods Analytics Textiles Software Systems Labs Partners Consulting Energy Motors Pharma "
           "Health Capital Finance Media Studios Robotics Networks Telecom Farms Supply Freight Marine Aviation "
           "Construction Engineering Materials Chemicals Plastics Metals Mining Retail Apparel Furniture "
           "Electronics Devices Security Insurance Holdings Ventures Solutions Services Industries Group "
           "Trading Exports").split()
UNICODE_NAMES = ("Société Générale des Eaux", "Müller Präzisionstechnik", "Øresund Shipping", "Łódź Textiles",
                 "Café Olé Holdings", "São Paulo Logística", "Zürich Analytik", "Москва Транс Логистика",
                 "東京ロボティクス", "नमस्ते फूड्स", "Ελληνική Ναυτιλία", "Kraków Złote Młyny")
SUFFIX_VARIANTS = {
    "Inc": ("Inc", "Inc.", "INC", "Incorporated"),
    "LLC": ("LLC", "Llc", "llc"),
    "Ltd": ("Ltd", "Ltd.", "Limited"),
    "Corp": ("Corp", "Corp.", "Corporation"),
    "Pvt Ltd": ("Pvt Ltd", "Pvt. Ltd.", "Private Limited", "PVT LTD"),
}
ROLE_LOCALS = ("info", "contact", "hello", "accounts", "admin", "office")
FIRST_NAMES = ("asha", "ben", "chen", "dina", "eli", "farah", "gus", "hana", "ivan", "jo", "kai", "lena", "marco",
               "nia", "omar", "priya", "quinn", "rosa", "sam", "tara", "uma", "vik", "wen", "yara", "zane")
LAST_NAMES = ("shah", "lee", "garcia", "okafor", "novak", "silva", "kim", "patel", "rossi", "mendes", "berg",
              "costa", "ito", "khan", "lopez", "moreau", "nair", "olsen", "park", "reyes")

BATCH1_START = datetime(2026, 9, 1)
LATE_BATCH = "crm_batch_002.csv"
# Sample profile: every scenario (and every variant of it) appears at least this often.
SAMPLE_MINIMUMS = {"malformed_value": 3, "missing_required": 3, "malformed_line": 2, "boundary_value": 6,
                   "one_to_many_conflict": 3, "manual_override": 2, "dirty_name": 2}


# Key normalization used for the labels ------------------------------------------------------
# This is the documented name rule (NFC, lowercase, non-alphanumerics to space, trailing
# legal suffixes dropped). It is written independently of the pipeline's implementation.
_SUFFIX_RE = re.compile(r"( (" + "|".join(NAME_SUFFIX_TOKENS) + r"))+$")


def name_key(display: str) -> str | None:
    text = unicodedata.normalize("NFC", display.strip()).lower()
    text = re.sub(r"[\W_]+", " ", text).strip()
    text = _SUFFIX_RE.sub("", text)
    return text or None


# Entities and records ----------------------------------------------------------------------

@dataclass(eq=False)
class Entity:
    """A real-world organization. In the master when cid is set, otherwise unknown to it."""

    cid: str | None
    base: str           # display name without legal suffix
    suffix: str
    tax: str            # canonical form, e.g. TX123456789
    email: str          # primary mailbox, lowercase
    domain: str
    phone: str          # E.164, e.g. +14155550134
    active: bool = True
    created_at: datetime | None = None
    records: list = field(default_factory=list)

    @property
    def legal_name(self) -> str:
        return f"{self.base} {self.suffix}"

    @property
    def key(self) -> str | None:
        return name_key(self.legal_name)


@dataclass(eq=False)
class Record:
    system: str
    entity: Entity | None       # the record's true entity (None for designed false positives)
    scenario: str = "clean"
    name: str | None = None     # display text before noise; None = absent
    email: str | None = None    # canonical form; None = absent
    phone: str | None = None
    domain: str | None = None
    tax: str | None = None
    canonical_ref: str | None = None
    whitespace: tuple = ()      # fields rendered as whitespace-only
    defect: str | None = None   # expected reject reason code for this record's row
    ts: datetime | None = None
    sid: str | None = None
    long_id: bool = False
    file: str = ""
    override: str | None = None  # canonical id of a steward override

    @property
    def keys(self) -> dict[str, str]:
        out = {}
        nk = name_key(self.name) if self.name else None
        if self.tax:
            out["exact_tax_id"] = self.tax
        if self.email:
            out["exact_email"] = self.email
        if nk and self.domain:
            out["rule_name_domain"] = f"{nk}|{self.domain}"
        if nk and self.phone:
            out["rule_phone_name"] = f"{self.phone}|{nk.split(' ')[0]}"
        return out

    @property
    def has_core_attribute(self) -> bool:
        return bool(self.tax or self.email or (self.name and name_key(self.name)))


class Generator:
    def __init__(self, n: int, seed: int, rates: dict[str, float], late_share: float, sample: bool):
        self.rng = Random(seed)
        self.seed = seed
        self.sample = sample
        self.rates = rates
        self.late_share = late_share
        additive = sum(rates.get(k, 0.0) for k in
                       ("exact_duplicate", "conflicting_duplicate", "unknown_source_system", "malformed_line", "missing_required"))
        self.base_n = max(30, round(n / (1 + additive)))
        self.canonicals: list[Entity] = []
        self.records: list[Record] = []
        self.extra_rows: list[dict] = []   # rows that are not base records (dupes, defects, noise)
        self.domains_used: dict[str, int] = {}
        self.serial = 0
        self.reserved: set[int] = set()    # id(entity) of entities already used by a scenario
        self.twins_email: list[tuple[Entity, Entity]] = []
        self.late_records: list[Record] = []

    # -- helpers ---------------------------------------------------------------------------
    def count(self, scenario: str) -> int:
        target = round(self.rates.get(scenario, 0.0) * self.base_n)
        return max(target, SAMPLE_MINIMUMS.get(scenario, 1)) if self.sample else target

    def _next_serial(self) -> int:
        self.serial += 1
        return self.serial

    def _tax(self, k: int) -> str:
        return f"TX{(k * 982451653 + 12345 + self.seed * 7) % 10**9:09d}"

    def _phone(self, k: int) -> str:
        v = (k * 7919 + 104729 + self.seed * 13) % 8_000_000
        return f"+1{200 + v // 10000}555{v % 10000:04d}"

    def _domain(self, base: str) -> str:
        slug = "".join(ch for ch in unicodedata.normalize("NFKD", base).lower() if ch.isascii() and ch.isalnum())
        slug = slug or "org"
        n = self.domains_used.get(slug, 0) + 1
        self.domains_used[slug] = n
        return f"{slug}{'' if n == 1 else n}.example"

    def _random_base(self) -> str:
        r = self.rng
        return f"{r.choice(WORDS_A)} {r.choice(WORDS_B)} {r.choice(WORDS_C)}"

    def new_entity(self, in_master: bool = True, base: str | None = None, suffix: str | None = None) -> Entity:
        k = self._next_serial()
        base = base or self._random_base()
        domain = self._domain(base)
        entity = Entity(
            cid=None, base=base, suffix=suffix or self.rng.choice(list(SUFFIX_VARIANTS)),
            tax=self._tax(k), email=f"{self.rng.choice(ROLE_LOCALS)}@{domain}", domain=domain, phone=self._phone(k),
            created_at=datetime(2020, 1, 1) + timedelta(days=self.rng.randrange(6 * 365)),
        )
        if in_master:
            entity.cid = f"can_{len(self.canonicals) + 1:06d}"
            self.canonicals.append(entity)
        return entity

    def alt_email(self, entity: Entity) -> str:
        return f"{self.rng.choice(FIRST_NAMES)}.{self.rng.choice(LAST_NAMES)}@{entity.domain}"

    def make_record(self, entity: Entity, system: str, scenario: str = "clean") -> Record:
        p, r = PRESENCE[system], self.rng
        rec = Record(system=system, entity=entity, scenario=scenario)
        rec.name = entity.legal_name if r.random() < p["name"] else None
        if r.random() < p["email"]:
            rec.email = entity.email if r.random() < p["primary_email"] else self.alt_email(entity)
        rec.phone = entity.phone if r.random() < p["phone"] else None
        rec.domain = entity.domain if r.random() < p["domain"] else None
        rec.tax = entity.tax if r.random() < p["tax"] else None
        if entity.cid and r.random() < p["ref"]:
            rec.canonical_ref = entity.cid if r.random() < 0.9 or len(self.canonicals) < 2 else r.choice(self.canonicals).cid
        entity.records.append(rec)
        self.records.append(rec)
        return rec

    def available(self, pred=lambda rec: True) -> list[Record]:
        out = [rec for rec in self.records
               if rec.scenario == "clean" and rec.entity is not None and rec.entity.cid
               and id(rec.entity) not in self.reserved and pred(rec)]
        self.rng.shuffle(out)
        return out

    # -- build -----------------------------------------------------------------------------
    def build(self) -> None:
        n_special = self.count("no_match") + self.count("weak_rule_false_positive") + self.count("duplicate_account")
        target = max(self.base_n - n_special, 10)
        # A quarter of the boundary budget goes to unicode names, spread evenly over the master.
        n_unicode = max(1, round(self.count("boundary_value") * 0.25)) if self.count("boundary_value") else 0
        step = max(3, round(target / 2.0 / max(n_unicode, 1)))
        made = unicode_made = 0
        self.unicode_canonicals = n_unicode
        while made < target:
            base = None
            if unicode_made < n_unicode and len(self.canonicals) % step == 2:
                base = UNICODE_NAMES[unicode_made % len(UNICODE_NAMES)]
                unicode_made += 1
            entity = self.new_entity(base=base)
            systems = [s for s in SYSTEMS if self.rng.random() < APPEAR[s]] or [self.rng.choice(SYSTEMS)]
            for system in systems:
                self.make_record(entity, system, "boundary_unicode_name" if base else "clean")
                made += 1
            if base:
                self.reserved.add(id(entity))
        for _ in range(max(1, len(self.canonicals) // 20)):   # master-only entities, some retired
            entity = self.new_entity()
            entity.active = self.rng.random() < 0.7

        self._inactive_only()
        self._one_to_many()
        self._boundary()
        self._dirty_names()
        self._malformed_values()
        self._invalid_refs()
        self._no_match()
        self._weak_false_positives()
        self._duplicate_accounts()
        self._overrides()
        self._timestamps_and_ids()
        self._late_batch()
        self._extra_rows()

    def _inactive_only(self) -> None:
        need = self.count("inactive_only")
        for rec in self.available():
            if need <= 0:
                break
            entity = rec.entity
            if id(entity) in self.reserved:
                continue
            entity.active = False
            self.reserved.add(id(entity))
            for r in entity.records:
                r.scenario = "inactive_only"
                need -= 1

    def _one_to_many(self) -> None:
        need, kind = self.count("one_to_many_conflict"), 0
        has_crm_tax = lambda rec: any(r.system == "crm" and r.tax for r in rec.entity.records if r is not rec)
        pools = [self.available(lambda rec: rec.system != "crm" and has_crm_tax(rec)), self.available()]
        for rec in pools[0] + pools[1]:
            if need <= 0:
                break
            x = rec.entity
            if id(x) in self.reserved or rec.scenario != "clean":
                continue
            if kind % 2 == 0:  # shared mailbox: twin canonical with the same primary email
                y = self.new_entity(base=self._random_base())
                y.email = x.email
                rec.tax, rec.email, rec.scenario = None, x.email, "one_to_many_email"
                self.twins_email.append((x, y))
            else:              # same name after suffix stripping, same website
                other_suffix = next(s for s in SUFFIX_VARIANTS if s != x.suffix)
                y = self.new_entity(base=x.base, suffix=other_suffix)
                y.domain = x.domain
                y.email = f"office@{x.domain}" if not x.email.startswith("office@") else f"info@{x.domain}"
                rec.tax, rec.email = None, None
                rec.name, rec.domain, rec.scenario = x.legal_name, x.domain, "one_to_many_name_domain"
            self.reserved.update({id(x), id(y)})
            if self.rng.random() < 0.5:
                twin_rec = self.make_record(y, "billing", "twin_entity")
                twin_rec.tax = y.tax
            kind += 1
            need -= 1

    def _boundary(self) -> None:
        need = max(0, self.count("boundary_value") - self.unicode_canonicals)
        variants = ("boundary_max_length", "boundary_whitespace_only", "boundary_embedded_newline", "boundary_long_id")
        for i, rec in enumerate(self.available()[:need]):
            variant = variants[i % len(variants)]
            rec.scenario = variant
            if variant == "boundary_max_length":
                entity = rec.entity
                filler = " ".join(self.rng.choice(WORDS_A + WORDS_B + WORDS_C) for _ in range(60))
                entity.base = (entity.base + " " + filler)[: 255 - len(entity.suffix) - 1].rstrip()
                entity.base = entity.base.ljust(255 - len(entity.suffix) - 1, "X")
                for r in entity.records:
                    if r.name:
                        r.name = entity.legal_name
                rec.name = entity.legal_name
                self.reserved.add(id(entity))
            elif variant == "boundary_whitespace_only":
                rec.whitespace = ("name", "email", "tax_id")
                rec.name = rec.email = rec.tax = None
            elif variant == "boundary_embedded_newline":
                words = rec.entity.legal_name.split(" ")
                rec.name = " ".join(words[:1]) + "\n" + " ".join(words[1:])
            else:
                rec.long_id = True

    def _dirty_names(self) -> None:
        for rec in self.available(lambda r: len(r.entity.base.split(" ")[0]) >= 4)[: self.count("dirty_name")]:
            first, rest = rec.entity.base.split(" ", 1)
            i = self.rng.randrange(1, len(first) - 1)
            typo = first[:i] + first[i + 1] + first[i] + first[i + 2:] if first[i] != first[i + 1] else first[:i] + first[i + 1:]
            rec.name = f"{typo} {rest} {rec.entity.suffix}"
            rec.tax, rec.phone = None, None
            rec.email = self.alt_email(rec.entity) if self.rng.random() < 0.5 else None
            rec.domain = rec.entity.domain
            rec.scenario = "dirty_name"

    def _malformed_values(self) -> None:
        codes = ("INVALID_EMAIL", "INVALID_PHONE", "TYPE_CAST_FAILED")
        for i, rec in enumerate(self.available()[: self.count("malformed_value")]):
            rec.defect, rec.scenario = codes[i % 3], "malformed_value"

    def _invalid_refs(self) -> None:
        for rec in self.available()[: self.count("invalid_canonical_ref")]:
            rec.canonical_ref = f"can_L{self.rng.randrange(10**7):07d}"
            rec.defect, rec.scenario = "INVALID_CANONICAL_REF", "invalid_canonical_ref"

    def _no_match(self) -> None:
        for _ in range(self.count("no_match")):
            entity = self.new_entity(in_master=False)
            self.make_record(entity, self.rng.choice(SYSTEMS), "no_match")

    def _weak_false_positives(self) -> None:
        targets = self.available(lambda r: r.entity.active)
        for z_rec in targets[: self.count("weak_rule_false_positive")]:
            z = z_rec.entity
            first = z.base.split(" ")[0]
            other = self._random_base().split(" ", 1)[1]
            while f"{first} {other}" == z.base:
                other = self._random_base().split(" ", 1)[1]
            rec = Record(system=self.rng.choice(SYSTEMS), entity=None, scenario="weak_rule_false_positive",
                         name=f"{first} {other} {self.rng.choice(list(SUFFIX_VARIANTS))}", phone=z.phone)
            self.records.append(rec)
            self.reserved.add(id(z))

    def _duplicate_accounts(self) -> None:
        for rec in self.available(lambda r: r.entity.active and (r.tax or r.email == r.entity.email))[: self.count("duplicate_account")]:
            twin = Record(system=rec.system, entity=rec.entity, scenario="duplicate_account", name=rec.name,
                          email=rec.email, phone=rec.phone, domain=rec.domain, tax=rec.tax)
            rec.scenario = "duplicate_account"
            rec.entity.records.append(twin)
            self.records.append(twin)
            self.reserved.add(id(rec.entity))

    def _overrides(self) -> None:
        conflicts = [r for r in self.records if r.scenario in ("one_to_many_email", "one_to_many_name_domain")]
        dirty = [r for r in self.records if r.scenario == "dirty_name"]
        self.rng.shuffle(conflicts)
        self.rng.shuffle(dirty)
        # Alternate between the two cases, leaving at least one designed conflict open.
        candidates = [r for pair in zip(dirty, conflicts[:-1]) for r in pair]
        candidates += dirty[len(candidates) // 2:] + conflicts[len(candidates) // 2:-1]
        for rec in candidates[: self.count("manual_override")]:
            rec.override = rec.entity.cid
            rec.scenario += "+override"

    def _timestamps_and_ids(self) -> None:
        span = 29 * 24 * 3600
        for rec in self.records:
            rec.ts = BATCH1_START + timedelta(seconds=self.rng.randrange(span))
            rec.file = f"{rec.system}_batch_001.csv"
        # schema drift and broken schema batches: fresh entities, newer than batch 1
        self.drift_records, self.broken_records = [], []
        for _ in range(max(5, round(0.01 * self.base_n))):
            rec = self.make_record(self.new_entity(), "support", "schema_drift")
            rec.ts, rec.file, rec.canonical_ref = datetime(2026, 10, 5) + timedelta(minutes=self.rng.randrange(1440)), "schema_drift_batch.csv", None
            self.drift_records.append(rec)
        for _ in range(max(5, round(0.002 * self.base_n))):
            rec = self.make_record(self.new_entity(), "billing", "broken_schema")
            rec.ts, rec.file = datetime(2026, 10, 6), "broken_schema_batch.csv"
            self.broken_records.append(rec)
        self.missing_ts_records = []
        for i in range(self.count("missing_required")):
            if i % 3 == 2:   # one in three: the key exists but source_updated_at is null
                entity = self.new_entity(in_master=False)
                rec = Record(system=self.rng.choice(SYSTEMS), entity=entity, scenario="missing_required",
                             name=entity.legal_name, email=entity.email, defect="MISSING_REQUIRED_FIELD")
                rec.ts, rec.file = None, f"{rec.system}_batch_001.csv"
                self.records.append(rec)
                self.missing_ts_records.append(rec)
        for system in SYSTEMS:
            recs = [r for r in self.records if r.system == system]
            self.rng.shuffle(recs)
            for i, rec in enumerate(recs, start=1):
                rec.sid = f"{ID_PREFIX[system]}-{i:07d}"
                if rec.long_id:
                    rec.sid = (f"{ID_PREFIX[system]}-LONG-{i:07d}-").ljust(64, "Z")

    def _late_batch(self) -> None:
        crm_batch1 = [r for r in self.records if r.file == "crm_batch_001.csv" and r.defect is None]
        size = max(4 if self.sample else 0, round(self.late_share * len(crm_batch1)))
        plan = {"late": round(size * 0.6), "remap": round(size * 0.2), "conflict_retained": round(size * 0.1),
                "unresolved_then_mapped": round(size * 0.1)}
        if self.sample:
            plan = {k: max(1, v) for k, v in plan.items()}
        index, active = self.index()
        used: set[int] = set()

        def outcome_of(rec: Record) -> str:
            return expected_outcome(rec, index, active)[0]

        mapped = [r for r in crm_batch1 if r.scenario in ("clean", "boundary_unicode_name") and outcome_of(r) == "MAPPED"]
        self.rng.shuffle(mapped)
        spare = [e for e in self.canonicals if e.active and id(e) not in self.reserved and e.records]
        self.rng.shuffle(spare)

        def take(pool):
            for rec in pool:
                if id(rec) not in used:
                    used.add(id(rec))
                    return rec
            return None

        def new_version(rec: Record, scenario: str, ts: datetime, **attrs) -> Record:
            version = Record(system="crm", entity=attrs.pop("entity", rec.entity), scenario=scenario, sid=rec.sid,
                             name=attrs.get("name", rec.name), email=attrs.get("email", rec.email),
                             phone=attrs.get("phone", rec.phone), domain=attrs.get("domain", rec.domain),
                             tax=attrs.get("tax", rec.tax), ts=ts, file=LATE_BATCH)
            self.late_records.append(version)
            return version

        for _ in range(plan["late"]):
            rec = take(mapped)
            if rec is None or not spare:
                break
            w = spare[len(self.late_records) % len(spare)]
            new_version(rec, "late_arrival", rec.ts - timedelta(days=self.rng.randint(5, 20)),
                        tax=w.tax, email=None, name=w.legal_name, domain=None, phone=None, entity=rec.entity)
        for _ in range(plan["remap"]):
            rec = take(mapped)
            if rec is None or not spare:
                break
            w = spare.pop()
            self.reserved.add(id(w))
            new_version(rec, "remap", datetime(2026, 10, 2) + timedelta(hours=self.rng.randrange(24 * 20)),
                        tax=w.tax, name=w.legal_name, email=None, domain=w.domain, phone=None, entity=w)
        email_twinned = {id(x) for x, _ in self.twins_email}
        retained_pool = [r for r in crm_batch1 if id(r.entity) in email_twinned and r.tax and r.scenario == "clean"
                         and outcome_of(r) == "MAPPED"]
        for _ in range(plan["conflict_retained"]):
            rec = take(retained_pool)
            if rec is None:
                break
            new_version(rec, "conflict_keeps_mapping", datetime(2026, 10, 3) + timedelta(hours=self.rng.randrange(24 * 20)),
                        tax=None, email=rec.entity.email)
        unresolved_pool = [r for r in crm_batch1 if r.entity is not None and r.entity.cid and r.entity.active
                           and r.override is None and outcome_of(r) == "UNRESOLVED"]
        self.rng.shuffle(unresolved_pool)
        for _ in range(plan["unresolved_then_mapped"]):
            rec = take(unresolved_pool)
            if rec is None:
                break
            new_version(rec, "unresolved_then_mapped", datetime(2026, 10, 4) + timedelta(hours=self.rng.randrange(24 * 20)),
                        tax=rec.entity.tax)

    def _extra_rows(self) -> None:
        """Rows that are not base records: duplicates, superseded versions, noise."""
        batch1 = [r for r in self.records if r.file.endswith("_batch_001.csv") and r.defect is None and r.ts]
        self.rng.shuffle(batch1)
        n_dup, n_sup = self.count("exact_duplicate"), self.count("conflicting_duplicate")
        self.duplicate_of = batch1[:n_dup]
        self.superseded = []
        for rec in batch1[n_dup:n_dup + n_sup]:
            other = self.rng.choice(self.canonicals)
            older = Record(system=rec.system, entity=rec.entity, scenario="superseded_version", sid=rec.sid,
                           name=other.legal_name, email=other.email, tax=other.tax,
                           ts=rec.ts - timedelta(hours=self.rng.randint(1, 72)), file=rec.file, defect="DUPLICATE_KEY_SUPERSEDED")
            self.superseded.append(older)
        files = [f"{s}_batch_001.csv" for s in SYSTEMS]
        self.noise: list[tuple[str, list[str] | str, str]] = []  # (file, fields or raw line, code)
        for i in range(self.count("unknown_source_system")):
            entity = self.new_entity(in_master=False)
            fields = ["erp" if i % 2 == 0 else "legacy", f"ERP-{i + 1:06d}", entity.legal_name, entity.email, "", "", "",
                      "", iso(BATCH1_START + timedelta(days=3, minutes=i))]
            self.noise.append((self.rng.choice(files), fields, "UNKNOWN_SOURCE_SYSTEM"))
        for i in range(self.count("missing_required")):
            if i % 3 == 2:
                continue  # emitted as a keyed record in _timestamps_and_ids
            entity = self.new_entity(in_master=False)
            system = self.rng.choice(SYSTEMS)
            fields = [system, f"{ID_PREFIX[system]}-X{i:06d}", entity.legal_name, entity.email, "", "", "", "",
                      iso(BATCH1_START + timedelta(days=4, minutes=i))]
            fields[1 if i % 3 == 0 else 0] = ""
            self.noise.append((f"{system}_batch_001.csv", fields, "MISSING_REQUIRED_FIELD"))
        for i in range(self.count("malformed_line")):
            entity = self.new_entity(in_master=False)
            system = self.rng.choice(SYSTEMS)
            if i % 2 == 0:  # unquoted comma inside the name: one column too many
                raw = (f"{system},{ID_PREFIX[system]}-M{i:06d},{entity.base}, {entity.suffix},{entity.email},,,,,"
                       f"{iso(BATCH1_START + timedelta(days=5, minutes=i))}")
            else:           # truncated line
                raw = f"{system},{ID_PREFIX[system]}-M{i:06d},{entity.base}"
            self.noise.append((f"{system}_batch_001.csv", raw, "MALFORMED_ROW"))

    # -- expected outcomes -----------------------------------------------------------------
    def index(self) -> tuple[dict[str, dict[str, list[str]]], dict[str, bool]]:
        idx: dict[str, dict[str, list[str]]] = {r: {} for r in RULE_ORDER[1:]}
        for e in self.canonicals:
            rec = Record(system="", entity=e, name=e.legal_name, email=e.email, phone=e.phone, domain=e.domain, tax=e.tax)
            for rule, key in rec.keys.items():
                idx[rule].setdefault(key, []).append(e.cid)
        return idx, {e.cid: e.active for e in self.canonicals}


def expected_outcome(rec: Record, index, active) -> tuple[str, str | None, str | None, str | None]:
    """(status, canonical_id, rule, unresolved_reason) by the documented rule order."""
    saw_inactive = False
    keys = rec.keys
    for rule in RULE_ORDER:
        if rule == "manual_override":
            hits = [rec.override] if rec.override else []
        else:
            hits = index[rule].get(keys.get(rule), []) if rule in keys else []
        live = sorted({h for h in hits if active[h]})
        saw_inactive = saw_inactive or any(not active[h] for h in hits)
        if len(live) == 1:
            return "MAPPED", live[0], rule, None
        if len(live) > 1:
            return "CONFLICT", None, rule, None
    if saw_inactive:
        return "UNRESOLVED", None, None, "INACTIVE_CANONICAL_ONLY"
    if not rec.has_core_attribute:
        return "UNRESOLVED", None, None, "INSUFFICIENT_ATTRIBUTES"
    return "UNRESOLVED", None, None, "NO_CANDIDATE"


# Rendering -------------------------------------------------------------------------------

def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


class Renderer:
    """Turns canonical-form values into noisy text that normalizes back to the same key."""

    def __init__(self, rng: Random):
        self.rng = rng

    def name(self, text: str) -> str:
        r = self.rng
        if not text.isascii():
            return r.choice([text, unicodedata.normalize("NFD", text), text.lower(), f"  {text} "])
        words = text.split(" ")
        base, suffix = " ".join(words[:-1]), words[-1]
        for canonical, variants in sorted(SUFFIX_VARIANTS.items(), key=lambda kv: -len(kv[0])):
            if text.endswith(" " + canonical):
                base, suffix = text[: -len(canonical) - 1], r.choice(variants)
                break
        choice = r.randrange(8)
        if choice == 0:
            return f"{base} {suffix}"
        if choice == 1:
            return f"{base} {suffix}".upper()
        if choice == 2:
            return f"{base} {suffix}".lower()
        if choice == 3:
            return f"{base}, {suffix}"
        if choice == 4:
            return base
        if choice == 5:
            return f"{base}  {suffix}".replace(" ", "  ", 1)
        if choice == 6:
            return f"  {base} {suffix} "
        return f"{base} {suffix}".replace(" ", "-", 1) if "\n" not in base.split(" ")[0] else f"{base} {suffix}"

    def email(self, value: str) -> str:
        local, domain = value.split("@")
        return self.rng.choice([value, value.upper(), f"{local.capitalize()}@{domain}", f" {value} "])

    def phone(self, e164: str) -> str:
        a, e, l = e164[2:5], e164[5:8], e164[8:]
        return self.rng.choice([f"+1 {a} {e} {l}", f"({a}) {e}-{l}", f"{a}.{e}.{l}", f"{a}{e}{l}",
                                f"+1-{a}-{e}-{l}", f"001 {a} {e} {l}", f"1 {a} {e} {l}"])

    def domain(self, value: str) -> str:
        return self.rng.choice([value, f"www.{value}", f"https://{value}/", f"http://www.{value.upper()}",
                                f"{value}/about"])

    def tax(self, value: str) -> str:
        d = value[2:]
        return self.rng.choice([f"TX-{d[:3]}-{d[3:6]}-{d[6:]}", value.lower(), f"TX {d[:3]} {d[3:6]} {d[6:]}", f" {value} "])

    def timestamp(self, ts: datetime) -> str:
        choice = self.rng.randrange(10)
        if choice < 7:
            return iso(ts)
        if choice < 9:
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        local = ts.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
        return local.strftime("%Y-%m-%dT%H:%M:%S+05:30")

    def record(self, rec: Record) -> list[str]:
        r = self.rng
        ws = lambda: r.choice(["   ", "\t", "   "])
        fields = {
            "source_system": rec.system,
            "source_id": rec.sid,
            "name": self.name(rec.name) if rec.name else "",
            "email": self.email(rec.email) if rec.email else "",
            "phone": self.phone(rec.phone) if rec.phone else "",
            "domain": self.domain(rec.domain) if rec.domain else "",
            "tax_id": self.tax(rec.tax) if rec.tax else "",
            "canonical_ref": rec.canonical_ref or "",
            "source_updated_at": self.timestamp(rec.ts) if rec.ts else "",
        }
        for f in rec.whitespace:
            fields[f] = ws()
        if rec.defect == "INVALID_EMAIL":
            fields["email"] = r.choice(["first.last@", "no-at-sign.example", "a@b", "two@@signs.example"])
        elif rec.defect == "INVALID_PHONE":
            fields["phone"] = r.choice(["555-01", "call-me", "+1 (415) 555-01XX", "12345"])
        elif rec.defect == "TYPE_CAST_FAILED":
            fields["source_updated_at"] = r.choice(["2026-13-45T10:00:00Z", "31/12/2025 10:00", "not-a-date", "epoch"])
        return [fields[c] for c in CONTRACT]


def csv_line(fields: list[str]) -> str:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(fields)
    return buf.getvalue()


class FileWriter:
    """Writes a CSV while tracking the physical start line of every row."""

    def __init__(self, path: Path, header: list[str]):
        self.path, self.header = path, header
        self.chunks = [csv_line(header)]
        self.line = 2

    def add(self, text: str) -> int:
        start = self.line
        self.chunks.append(text)
        self.line += text.count("\n")
        return start

    def close(self) -> None:
        self.path.write_text("".join(self.chunks), encoding="utf-8", newline="")


def write_outputs(gen: Generator, out: Path, expected_out: Path) -> dict:
    rng = Random(gen.seed + 1)
    render = Renderer(rng)
    out.mkdir(parents=True, exist_ok=True)
    expected_out.mkdir(parents=True, exist_ok=True)
    for stale in list(out.glob("*_batch*.csv")):
        stale.unlink()

    # Reference data.
    with (out / "canonical_entity.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["canonical_id", "legal_name", "tax_id", "primary_email", "domain", "phone", "is_active", "created_at"])
        for e in gen.canonicals:
            d, p = e.tax[2:], e.phone
            w.writerow([e.cid, e.legal_name, f"TX-{d[:3]}-{d[3:6]}-{d[6:]}", e.email, e.domain,
                        f"+1 {p[2:5]}-{p[5:8]}-{p[8:]}", str(e.active).lower(), iso(e.created_at)])
    with (out / "mapping_override.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["source_system", "source_id", "canonical_id", "reason", "created_at"])
        for rec in sorted((r for r in gen.records if r.override), key=lambda r: (r.system, r.sid)):
            reason = "steward resolved conflict" if "one_to_many" in rec.scenario else "steward linked dirty record"
            w.writerow([rec.system, rec.sid, rec.override, reason, "2026-08-15T09:00:00Z"])

    # Source batches: group rows per file, shuffle, write with line tracking.
    rows: dict[str, list[tuple[str, Record | None, str | None]]] = {}
    rendered: dict[int, str] = {}
    for rec in gen.records:
        rendered[id(rec)] = csv_line(render.record(rec))
        rows.setdefault(rec.file, []).append((rendered[id(rec)], rec, rec.defect))
    for rec in gen.duplicate_of:
        rows[rec.file].append((rendered[id(rec)], None, None))
    for rec in gen.superseded:
        rows[rec.file].append((csv_line(render.record(rec)), rec, rec.defect))
    for file, payload, code in gen.noise:
        text = payload + "\n" if isinstance(payload, str) else csv_line(payload)
        rows[file].append((text, None, code))
    for rec in gen.late_records:
        rows.setdefault(rec.file, []).append((csv_line(render.record(rec)), rec, None))

    expected_quarantine = []
    for file in sorted(rows):
        entries = rows[file]
        rng.shuffle(entries)
        header = list(CONTRACT)
        if file == "broken_schema_batch.csv":
            header = [c for c in CONTRACT if c != "source_updated_at"]
        elif file == "schema_drift_batch.csv":
            header = ["source_id", "source_system", "loyalty_tier", "name", "email", "phone", "domain", "tax_id", "source_updated_at"]
        writer = FileWriter(out / file, header)
        for text, rec, code in entries:
            if file == "broken_schema_batch.csv":
                fields = next(csv.reader(io.StringIO(text)))
                text = csv_line(fields[:-1])
            elif file == "schema_drift_batch.csv":
                f = dict(zip(CONTRACT, next(csv.reader(io.StringIO(text)))))
                f["loyalty_tier"] = rng.choice(["gold", "silver", "bronze", ""])
                text = csv_line([f[c] for c in header])
            line = writer.add(text)
            if code and file != "broken_schema_batch.csv":
                expected_quarantine.append((file, line, code))
        writer.close()

    # Expected final state: replay the documented semantics over files in processing order.
    index, active = gen.index()
    by_file: dict[str, list[Record]] = {}
    for rec in gen.records + gen.late_records:
        if rec.defect is None and rec.file != "broken_schema_batch.csv":
            by_file.setdefault(rec.file, []).append(rec)
    state: dict[tuple[str, str], dict] = {}
    late = 0
    for file in sorted(by_file):
        for rec in sorted(by_file[file], key=lambda r: (r.system, r.sid)):
            key = (rec.system, rec.sid)
            status, cid, rule, reason = expected_outcome(rec, index, active)
            prev = state.get(key)
            if prev and rec.ts < prev["ts"]:
                late += 1
                prev["scenario"] = "late_arrival"
                continue
            cur, cur_rule = (prev or {}).get("canonical"), (prev or {}).get("mapping_rule")
            if status == "MAPPED" and cid != cur:
                cur, cur_rule = cid, rule
            truth = rec.entity.cid if rec.entity is not None else None
            state[key] = {"ts": rec.ts, "status": status, "canonical": cur, "mapping_rule": cur_rule,
                          "decision_rule": rule, "truth": truth, "scenario": rec.scenario}
    for rec in gen.records:
        key = (rec.system, rec.sid)
        if rec.defect and rec.file != "broken_schema_batch.csv" and key not in state:
            state[key] = {"status": "QUARANTINED", "canonical": None, "mapping_rule": None, "decision_rule": None,
                          "truth": rec.entity.cid if rec.entity else None, "scenario": rec.scenario}

    with (expected_out / "expected_labels.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["source_system", "source_id", "expected_status", "expected_canonical_id", "expected_rule",
                    "true_canonical_id", "scenario"])
        for (system, sid), st in sorted(state.items()):
            scenario = st["scenario"]
            if scenario == "clean" and st["status"] != "MAPPED":
                scenario = {"CONFLICT": "shared_identifier", "UNRESOLVED": "sparse_attributes"}.get(st["status"], scenario)
            rule = st["mapping_rule"] or (st["decision_rule"] if st["status"] == "CONFLICT" else None)
            w.writerow([system, sid, st["status"], st["canonical"] or "", rule or "", st["truth"] or "", scenario])
    with (expected_out / "expected_quarantine.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["source_file", "source_row_num", "expected_reason_code"])
        w.writerows(sorted(expected_quarantine))

    current = [(k[0], st["canonical"]) for k, st in state.items() if st.get("canonical")]
    per_target: dict[tuple[str, str], int] = {}
    for item in current:
        per_target[item] = per_target.get(item, 0) + 1
    statuses: dict[str, int] = {}
    for st in state.values():
        statuses[st["status"]] = statuses.get(st["status"], 0) + 1
    scenarios: dict[str, int] = {}
    for rec in gen.records + gen.late_records + gen.superseded:
        scenarios[rec.scenario] = scenarios.get(rec.scenario, 0) + 1
    codes: dict[str, int] = {}
    for _, _, code in expected_quarantine:
        codes[code] = codes.get(code, 0) + 1
    summary = {
        "seed": gen.seed,
        "base_records": gen.base_n,
        "canonical_entities": len(gen.canonicals),
        "active_canonicals": sum(e.active for e in gen.canonicals),
        "manual_overrides": sum(1 for r in gen.records if r.override),
        "source_rows_by_file": {f: len(v) for f, v in sorted(rows.items())},
        "exact_duplicate_rows": len(gen.duplicate_of),
        "late_arrival_records": late,
        "many_to_one_keys": sum(n for n in per_target.values() if n > 1),
        "expected_status_counts": dict(sorted(statuses.items())),
        "expected_quarantine_by_code": dict(sorted(codes.items())),
        "scenario_record_counts": dict(sorted(scenarios.items())),
    }
    with (expected_out / "expected_summary.json").open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(summary, indent=2) + "\n")
    return summary


def load_generator_config(path: Path) -> tuple[dict[str, float], float]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    gen = raw.get("generator") or {}
    return {k: float(v) for k, v in (gen.get("rates") or {}).items()}, float(gen.get("late_batch_share", 0.05))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--n", type=int, default=100_000, help="approximate source rows across the three main batches")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "data" / "generated")
    parser.add_argument("--expected-out", type=Path, help="label directory (default <out>/expected)")
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "config" / "pipeline.yaml")
    parser.add_argument("--sample", action="store_true", help="small curated profile (~50 rows/source, every scenario at least once)")
    args = parser.parse_args(argv)
    rates, late_share = load_generator_config(args.config)
    n = 150 if args.sample else args.n
    gen = Generator(n=n, seed=args.seed, rates=rates, late_share=late_share, sample=args.sample)
    gen.build()
    summary = write_outputs(gen, args.out, args.expected_out or args.out / "expected")
    rows = sum(summary["source_rows_by_file"].values())
    print(f"generated {rows} source rows in {len(summary['source_rows_by_file'])} files, "
          f"{summary['canonical_entities']} canonical entities -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
