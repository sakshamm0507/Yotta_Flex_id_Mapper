"""Attribute normalization, defined twice on purpose.

The pipeline normalizes in SQL: DuckDB macros registered on every connection, so
a batch is normalized set-based inside the database. The Python functions below
implement the same rules. They drive the row-by-row baseline (`baseline.py`), and
`tests/test_normalize.py` asserts that the two implementations agree.

| Field   | Rule                                                                  |
|---------|-----------------------------------------------------------------------|
| any     | trim whitespace (Unicode-aware); empty or whitespace-only -> NULL     |
| email   | lowercase                                                             |
| phone   | digits only, '+' and country code prefixed (E.164 style)              |
| name    | NFC, lowercase, non-alphanumerics -> space, collapse, drop suffixes   |
| domain  | lowercase, strip scheme, 'www.', path/query, port, edge dots          |
| tax_id  | uppercase, strip whitespace and hyphens                               |
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

import duckdb

from idmap.config import Config

# Characters Python's str.strip() removes. The SQL class below matches the same set,
# so both implementations treat NBSP, tabs and other Unicode spaces as whitespace.
_SQL_WS = r"[\t\n\x0b\f\r\x1c-\x1f\x85\p{Z}]"

EMAIL_PATTERN = r"^[A-Za-z0-9._%+'-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"
EMAIL_MAX_LENGTH = 254  # RFC 5321 path limit
PHONE_PATTERN = r"^\+?[0-9() .-]+$"
ISO_TIMESTAMP_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?)?(Z|[+-][0-9]{2}(:?[0-9]{2})?)?$"

_EMAIL_RE = re.compile(EMAIL_PATTERN)
_PHONE_RE = re.compile(PHONE_PATTERN)
_NON_ALNUM_RE = re.compile(r"[\W_]+")
_NON_DIGIT_RE = re.compile(r"[^0-9]")
_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.-]*://")
_WWW_RE = re.compile(r"^www\.")
_PATH_RE = re.compile(r"[/?#].*$")
_PORT_RE = re.compile(r":[0-9]+$")
_TAX_STRIP_RE = re.compile(r"[\s-]")


def suffix_pattern(suffixes: Iterable[str]) -> str:
    """Regex that removes trailing legal-form tokens, e.g. 'acme pvt ltd' -> 'acme'."""
    alternatives = "|".join(sorted({s.lower() for s in suffixes}, key=lambda s: (-len(s), s)))
    return f"( ({alternatives}))+$"


def _lower(value: str) -> str:
    # Python applies context-sensitive special casing ('İ' -> 'i̇', final sigma).
    # DuckDB uses simple case mapping, so map those two code points first.
    return value.replace("İ", "i").replace("Σ", "σ").lower()


def clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def email_norm(value: str | None) -> str | None:
    value = clean(value)
    return _lower(value) if value else None


def email_is_valid(value: str | None) -> bool:
    value = clean(value)
    return value is None or (len(value) <= EMAIL_MAX_LENGTH and bool(_EMAIL_RE.match(value)))


def phone_norm(value: str | None, country_code: str) -> str | None:
    value = clean(value)
    if value is None:
        return None
    digits = _NON_DIGIT_RE.sub("", value)
    if not digits:
        return None
    if value.startswith("+"):
        return "+" + digits
    if digits.startswith("00"):
        return "+" + digits[2:]
    if len(digits) == 10:
        return "+" + country_code + digits
    if len(digits) == 11 and digits.startswith("0"):
        return "+" + country_code + digits[1:]
    return "+" + digits


def phone_is_valid(value: str | None, country_code: str, min_digits: int, max_digits: int) -> bool:
    value = clean(value)
    if value is None:
        return True
    if not _PHONE_RE.match(value):
        return False
    norm = phone_norm(value, country_code)
    return norm is not None and min_digits <= len(norm) - 1 <= max_digits


def name_norm(value: str | None, suffix_regex: re.Pattern[str]) -> str | None:
    value = clean(value)
    if value is None:
        return None
    value = _lower(unicodedata.normalize("NFC", value))
    value = _NON_ALNUM_RE.sub(" ", value).strip()
    value = suffix_regex.sub("", value)
    return value or None


def domain_norm(value: str | None) -> str | None:
    value = clean(value)
    if value is None:
        return None
    value = _lower(value)
    value = _SCHEME_RE.sub("", value)
    value = _WWW_RE.sub("", value)
    value = _PATH_RE.sub("", value)
    value = _PORT_RE.sub("", value)
    value = value.strip(".")
    return value or None


def tax_id_norm(value: str | None) -> str | None:
    value = clean(value)
    if value is None:
        return None
    value = _TAX_STRIP_RE.sub("", value).upper()
    return value or None


class PyNormalizer:
    """Bundle of the Python rules bound to one config (used by the baseline)."""

    def __init__(self, cfg: Config):
        self.country_code = cfg.default_country_code
        self.suffix_regex = re.compile(suffix_pattern(cfg.name_suffixes))

    def row(self, name, email, phone, domain, tax_id) -> tuple[str | None, ...]:
        return (
            tax_id_norm(tax_id),
            email_norm(email),
            name_norm(name, self.suffix_regex),
            domain_norm(domain),
            phone_norm(phone, self.country_code),
        )


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def sql_macros(cfg: Config) -> list[str]:
    """DuckDB macro definitions implementing the rules above."""
    cc = _sql_literal(cfg.default_country_code)
    suffix_re = _sql_literal(suffix_pattern(cfg.name_suffixes))
    ws = _sql_literal(f"^{_SQL_WS}+|{_SQL_WS}+$")
    tax_strip = _sql_literal(f"[{_SQL_WS[1:-1]}-]")
    return [
        f"CREATE OR REPLACE TEMP MACRO idm_clean(x) AS NULLIF(regexp_replace(x, {ws}, '', 'g'), '')",
        "CREATE OR REPLACE TEMP MACRO idm_email_norm(x) AS lower(idm_clean(x))",
        f"""CREATE OR REPLACE TEMP MACRO idm_email_valid(x) AS (
                idm_clean(x) IS NULL
                OR (length(idm_clean(x)) <= {EMAIL_MAX_LENGTH}
                    AND regexp_full_match(idm_clean(x), {_sql_literal(EMAIL_PATTERN)})))""",
        f"""CREATE OR REPLACE TEMP MACRO idm_phone_norm(x) AS (
                CASE
                    WHEN idm_clean(x) IS NULL THEN NULL
                    WHEN regexp_replace(x, '[^0-9]', '', 'g') = '' THEN NULL
                    WHEN starts_with(idm_clean(x), '+') THEN '+' || regexp_replace(x, '[^0-9]', '', 'g')
                    WHEN starts_with(regexp_replace(x, '[^0-9]', '', 'g'), '00')
                        THEN '+' || substr(regexp_replace(x, '[^0-9]', '', 'g'), 3)
                    WHEN length(regexp_replace(x, '[^0-9]', '', 'g')) = 10
                        THEN '+' || {cc} || regexp_replace(x, '[^0-9]', '', 'g')
                    WHEN length(regexp_replace(x, '[^0-9]', '', 'g')) = 11
                         AND starts_with(regexp_replace(x, '[^0-9]', '', 'g'), '0')
                        THEN '+' || {cc} || substr(regexp_replace(x, '[^0-9]', '', 'g'), 2)
                    ELSE '+' || regexp_replace(x, '[^0-9]', '', 'g')
                END)""",
        f"""CREATE OR REPLACE TEMP MACRO idm_phone_valid(x) AS (
                idm_clean(x) IS NULL
                OR (regexp_full_match(idm_clean(x), {_sql_literal(PHONE_PATTERN)})
                    AND idm_phone_norm(x) IS NOT NULL
                    AND length(idm_phone_norm(x)) - 1 BETWEEN {cfg.phone_min_digits} AND {cfg.phone_max_digits}))""",
        f"""CREATE OR REPLACE TEMP MACRO idm_name_norm(x) AS NULLIF(
                regexp_replace(
                    trim(regexp_replace(lower(nfc_normalize(idm_clean(x))), '[^\\p{{L}}\\p{{N}}]+', ' ', 'g')),
                    {suffix_re}, ''),
                '')""",
        """CREATE OR REPLACE TEMP MACRO idm_domain_norm(x) AS NULLIF(
                trim(
                    regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                        lower(idm_clean(x)),
                        '^[a-z][a-z0-9+.-]*://', ''), '^www\\.', ''), '[/?#].*$', ''), ':[0-9]+$', ''),
                    '.'),
                '')""",
        f"CREATE OR REPLACE TEMP MACRO idm_tax_norm(x) AS NULLIF(upper(regexp_replace(idm_clean(x), {tax_strip}, '', 'g')), '')",
        f"""CREATE OR REPLACE TEMP MACRO idm_parse_ts(x) AS (
                CASE WHEN regexp_full_match(idm_clean(x), {_sql_literal(ISO_TIMESTAMP_PATTERN)})
                     THEN TRY_CAST(idm_clean(x) AS TIMESTAMPTZ)::TIMESTAMP END)""",
    ]


def register_macros(con: duckdb.DuckDBPyConnection, cfg: Config) -> None:
    for statement in sql_macros(cfg):
        con.execute(statement)


def normalize_batch(con: duckdb.DuckDBPyConnection) -> int:
    """t_dedup -> t_stage: add the normalized matching attributes (set-based, in SQL)."""
    con.execute(
        """CREATE OR REPLACE TEMP TABLE t_stage AS
           SELECT *,
                  idm_tax_norm(tax_id) AS tax_id_norm,
                  idm_email_norm(email) AS email_norm,
                  idm_phone_norm(phone) AS phone_norm,
                  idm_name_norm(name) AS name_norm,
                  idm_domain_norm(domain) AS domain_norm
           FROM t_dedup"""
    )
    return con.execute("SELECT count(*) FROM t_stage").fetchone()[0]
