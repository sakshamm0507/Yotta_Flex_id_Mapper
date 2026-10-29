"""Load and validate the pipeline configuration (config/pipeline.yaml)."""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Repository root: src/idmap/config.py -> parents[2].
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "pipeline.yaml"

# The rule engine implements exactly these rules; config only orders and toggles them.
RULE_IDS = (
    "manual_override",
    "exact_tax_id",
    "exact_email",
    "rule_name_domain",
    "rule_phone_name",
)


class ConfigError(ValueError):
    """Raised when pipeline.yaml is missing a key or holds an invalid value."""


@dataclass(frozen=True)
class Rule:
    id: str
    priority: int
    enabled: bool = True


@dataclass(frozen=True)
class Config:
    root: Path
    db_path: Path
    incoming_dir: Path
    incoming_pattern: str
    log_dir: Path
    report_file: Path
    canonical_file: Path
    override_file: Path | None
    expected_dir: Path | None
    allowed_source_systems: tuple[str, ...]
    max_reject_rate: float
    reject_rate_min_rows: int
    default_country_code: str
    phone_min_digits: int
    phone_max_digits: int
    name_suffixes: tuple[str, ...]
    rules: tuple[Rule, ...]
    thresholds: dict[str, float] = field(default_factory=dict)
    duckdb_threads: int | None = None
    duckdb_memory_limit: str | None = None
    generator: dict[str, Any] = field(default_factory=dict)

    @property
    def enabled_rules(self) -> tuple[Rule, ...]:
        return tuple(r for r in self.rules if r.enabled)

    def rule_priority(self, rule_id: str) -> int | None:
        for r in self.enabled_rules:
            if r.id == rule_id:
                return r.priority
        return None

    def replace(self, **changes: Any) -> Config:
        return dataclasses.replace(self, **changes)


def _get(section: dict[str, Any], key: str, where: str) -> Any:
    if key not in section:
        raise ConfigError(f"missing key '{where}.{key}' in pipeline config")
    return section[key]


def _path(root: Path, value: str | None) -> Path | None:
    if value in (None, ""):
        return None
    p = Path(value)
    return p if p.is_absolute() else root / p


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    """Read pipeline.yaml. `IDMAP_CONFIG` overrides the default location."""
    cfg_path = Path(path or os.environ.get("IDMAP_CONFIG") or DEFAULT_CONFIG_PATH).resolve()
    if not cfg_path.is_file():
        raise ConfigError(f"config file not found: {cfg_path}")
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    root = cfg_path.parent.parent

    paths = _get(raw, "paths", "")
    ref = _get(raw, "reference", "")
    val = _get(raw, "validation", "")
    norm = _get(raw, "normalization", "")
    matching = _get(raw, "matching", "")
    quality = raw.get("quality") or {}
    duck = raw.get("duckdb") or {}

    rules = []
    for item in _get(matching, "rules", "matching"):
        rule = Rule(id=str(item["id"]), priority=int(item["priority"]), enabled=bool(item.get("enabled", True)))
        if rule.id not in RULE_IDS:
            raise ConfigError(f"unknown matching rule '{rule.id}'; expected one of {RULE_IDS}")
        rules.append(rule)
    if len({r.id for r in rules}) != len(rules):
        raise ConfigError("matching.rules lists a rule more than once")

    max_reject_rate = float(_get(val, "max_reject_rate", "validation"))
    if not 0.0 < max_reject_rate <= 1.0:
        raise ConfigError("validation.max_reject_rate must be in (0, 1]")

    country_code = str(_get(norm, "default_country_code", "normalization"))
    if not country_code.isdigit():
        raise ConfigError("normalization.default_country_code must be digits only")

    suffixes = tuple(str(s).lower() for s in _get(norm, "name_suffixes", "normalization"))
    if not all(s.isalnum() for s in suffixes):
        raise ConfigError("normalization.name_suffixes must be alphanumeric tokens")

    systems = tuple(str(s).strip().lower() for s in _get(val, "allowed_source_systems", "validation"))
    if not systems:
        raise ConfigError("validation.allowed_source_systems must not be empty")

    return Config(
        root=root,
        db_path=_path(root, _get(paths, "db", "paths")),
        incoming_dir=_path(root, _get(paths, "incoming_dir", "paths")),
        incoming_pattern=str(paths.get("incoming_pattern", "*.csv")),
        log_dir=_path(root, _get(paths, "log_dir", "paths")),
        report_file=_path(root, _get(paths, "report_file", "paths")),
        canonical_file=_path(root, _get(ref, "canonical_file", "reference")),
        override_file=_path(root, ref.get("override_file")),
        expected_dir=_path(root, quality.get("expected_dir")),
        allowed_source_systems=systems,
        max_reject_rate=max_reject_rate,
        reject_rate_min_rows=int(val.get("reject_rate_min_rows", 0)),
        default_country_code=country_code,
        phone_min_digits=int(_get(norm, "phone_min_digits", "normalization")),
        phone_max_digits=int(_get(norm, "phone_max_digits", "normalization")),
        name_suffixes=suffixes,
        rules=tuple(sorted(rules, key=lambda r: (r.priority, r.id))),
        thresholds={k: float(v) for k, v in (quality.get("thresholds") or {}).items()},
        duckdb_threads=duck.get("threads"),
        duckdb_memory_limit=duck.get("memory_limit"),
        generator=raw.get("generator") or {},
    )
