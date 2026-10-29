"""Normalization rules: expected values, plus SQL/Python parity on tricky inputs."""

from __future__ import annotations

import re
import unicodedata

import duckdb
import pytest

from idmap import normalize as n
from idmap.config import load_config

CFG = load_config()
SUFFIX_RE = re.compile(n.suffix_pattern(CFG.name_suffixes))


@pytest.mark.parametrize("raw, expected", [
    (" A@X.COM ", "a@x.com"), ("a@x.com", "a@x.com"), ("   ", None), (None, None), (" Info@Acme.Example\t", "info@acme.example"),
])
def test_email_norm(raw, expected):
    assert n.email_norm(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("+1 (415) 555-0134", "+14155550134"),
    ("(415) 555-0134", "+14155550134"),
    ("415.555.0134", "+14155550134"),
    ("001 415 555 0134", "+14155550134"),
    ("1 415 555 0134", "+14155550134"),
    ("04155550134", "+14155550134"),       # national trunk prefix
    ("+44 20 7946 0958", "+442079460958"),
    ("call me", None), ("", None),
])
def test_phone_norm(raw, expected):
    assert n.phone_norm(raw, "1") == expected


@pytest.mark.parametrize("raw, valid", [
    ("+1 (415) 555-0134", True), ("555-01", False), ("call-me", False), ("+1 415 555 01XX", False),
    ("1234567890123456", False), (None, True), ("   ", True),
])
def test_phone_validity(raw, valid):
    assert n.phone_is_valid(raw, "1", CFG.phone_min_digits, CFG.phone_max_digits) is valid


@pytest.mark.parametrize("raw, valid", [
    ("a@b.co", True), ("first.last+tag@sub.example.org", True), ("first.last@", False), ("no-at-sign.example", False),
    ("a@b", False), ("two@@signs.example", False), (None, True), ("x" * 250 + "@b.co", False),
])
def test_email_validity(raw, valid):
    assert n.email_is_valid(raw) is valid


@pytest.mark.parametrize("raw, expected", [
    ("Acme Widgets, Inc.", "acme widgets"),
    ("ACME WIDGETS PVT. LTD.", "acme widgets"),
    ("Acme  Widgets   Private Limited", "acme widgets"),
    ("Acme-Widgets Corp", "acme widgets"),
    ("Inc", "inc"),                                    # a suffix alone is kept
    ("Corp Solutions Inc", "corp solutions"),          # only trailing suffixes are dropped
    ("Blue\nHarbor Logistics", "blue harbor logistics"),
    ("Société Générale", "société générale"),
    (unicodedata.normalize("NFD", "Société Générale"), "société générale"),
    ("東京ロボティクス", "東京ロボティクス"),
    ("  ...  ", None),
])
def test_name_norm(raw, expected):
    assert n.name_norm(raw, SUFFIX_RE) == expected


@pytest.mark.parametrize("raw, expected", [
    ("https://www.Acme.example/about?x=1", "acme.example"),
    ("http://acme.example:8080/", "acme.example"),
    ("WWW.ACME.EXAMPLE", "acme.example"),
    ("acme.example.", "acme.example"),
    ("https://", None),
])
def test_domain_norm(raw, expected):
    assert n.domain_norm(raw) == expected


@pytest.mark.parametrize("raw, expected", [("tx-100 0001", "TX1000001"), (" TX1000001 ", "TX1000001"), (" - ", None)])
def test_tax_id_norm(raw, expected):
    assert n.tax_id_norm(raw) == expected


PARITY_INPUTS = [
    None, "", "   ", "\t ", " A@X.COM ", "Info@Acme.Example", "+1 (415) 555-0134", "001 415 555 0134",
    "04155550134", "+44 20 7946 0958", "call me", "Acme Widgets, Inc.", "ACME WIDGETS PVT. LTD.",
    "Acme  Widgets   Private Limited", "Blue\nHarbor Logistics", "Société Générale des Eaux",
    unicodedata.normalize("NFD", "Société Générale des Eaux"), "MÜLLER PRÄZISIONSTECHNIK", "Øresund Shipping",
    "ŁÓDŹ TEXTILES", "Москва Транс Логистика", "東京ロボティクス", "नमस्ते फूड्स", "ΕΛΛΗΝΙΚΉ ΝΑΥΤΙΛΊΑ",
    "İstanbul Halı", "https://www.Acme.example/about?x=1", "http://acme.example:8080/", "tx-100 0001",
    "snake_case_name", "x" * 300, "Smith & Sons Corp.", "Ⅷ Holdings ²", " Em Space Corp　",
]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    n.register_macros(c, CFG)
    yield c
    c.close()


@pytest.mark.parametrize("value", PARITY_INPUTS)
def test_sql_and_python_normalization_agree(con, value):
    sql = con.execute(
        """SELECT idm_clean($v), idm_email_norm($v), idm_phone_norm($v), idm_name_norm($v), idm_domain_norm($v),
                  idm_tax_norm($v), idm_email_valid($v), idm_phone_valid($v)""",
        {"v": value},
    ).fetchone()
    py = (
        n.clean(value), n.email_norm(value), n.phone_norm(value, CFG.default_country_code),
        n.name_norm(value, SUFFIX_RE), n.domain_norm(value), n.tax_id_norm(value), n.email_is_valid(value),
        n.phone_is_valid(value, CFG.default_country_code, CFG.phone_min_digits, CFG.phone_max_digits),
    )
    assert sql == py


@pytest.mark.parametrize("raw, expected", [
    ("2026-09-01T10:00:00Z", "2026-09-01 10:00:00"),
    ("2026-09-01T15:30:00+05:30", "2026-09-01 10:00:00"),   # offsets convert to UTC
    ("2026-09-01 10:00:00", "2026-09-01 10:00:00"),         # naive = UTC
    ("2026-09-01", "2026-09-01 00:00:00"),
    ("2026-13-45T10:00:00Z", None), ("31/12/2025 10:00", None), ("epoch", None), ("infinity", None), ("", None),
])
def test_timestamp_parsing(con, raw, expected):
    con.execute("SET TimeZone = 'UTC'")
    value = con.execute("SELECT strftime(idm_parse_ts($v), '%Y-%m-%d %H:%M:%S')", {"v": raw}).fetchone()[0]
    assert value == expected
