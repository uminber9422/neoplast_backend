"""Unit tests for the data profiler — country / locale / language detection."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.pipeline.data_profiler import (
    ProspectProfile,
    apply_profile_to_prospect,
    language_for_country_code,
    profile_prospect,
    profile_prospect_obj,
)

# ---------------------------------------------------------------------------
# The headline Algeria scenario
# ---------------------------------------------------------------------------


def test_algeria_full_signal_set_max_confidence():
    """The user's example: infos@itp.dz + Algerian phone + 'Algeria' column."""
    profile = profile_prospect(
        email="infos@itp.dz",
        phone="+21321213412",
        country="Algeria",
    )
    assert profile.country == "Algeria"
    assert profile.country_code == "DZ"
    assert profile.search_locale == "dz"
    assert profile.language == "fr"
    # 0.6 + 0.4 + 0.3 = 1.3, capped at 1.0
    assert profile.confidence == 1.0
    # All three signals should appear in the trace
    assert any("csv_country" in s for s in profile.signals)
    assert any("phone_prefix" in s for s in profile.signals)
    assert any("email_tld" in s for s in profile.signals)


def test_algeria_email_tld_only():
    profile = profile_prospect(email="infos@itp.dz")
    assert profile.country_code == "DZ"
    assert profile.confidence == pytest.approx(0.3)


def test_algeria_phone_prefix_only_e164():
    profile = profile_prospect(phone="+213213412341")
    assert profile.country_code == "DZ"
    assert profile.confidence == pytest.approx(0.4)


def test_algeria_phone_prefix_only_00_format():
    """Algerian phone written with 00 prefix instead of +."""
    profile = profile_prospect(phone="0021321213412")
    assert profile.country_code == "DZ"


def test_algeria_country_alias_localized():
    """'Algerie' (no diacritic) should resolve to DZ."""
    profile = profile_prospect(country="Algerie")
    assert profile.country_code == "DZ"
    assert profile.country == "Algeria"


def test_algeria_country_with_diacritic():
    """'Algérie' (French spelling with diacritic) should resolve to DZ."""
    profile = profile_prospect(country="Algérie")
    assert profile.country_code == "DZ"


# ---------------------------------------------------------------------------
# India regression — must keep working unchanged
# ---------------------------------------------------------------------------


def test_india_full_signal_set():
    profile = profile_prospect(
        email="raj@example.in",
        phone="+919876543210",
        country="India",
    )
    assert profile.country_code == "IN"
    assert profile.search_locale == "in"
    assert profile.language == "en"
    assert profile.confidence == 1.0


def test_india_alias_bharat():
    assert profile_prospect(country="Bharat").country_code == "IN"


def test_india_iso_code_in_country_column():
    """User passed bare 'IN' as the country."""
    assert profile_prospect(country="IN").country_code == "IN"
    assert profile_prospect(country="in").country_code == "IN"


# ---------------------------------------------------------------------------
# Generic TLDs carry no signal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("email", [
    "ceo@acme.com",
    "founder@startup.io",
    "team@company.net",
    "info@charity.org",
])
def test_generic_tlds_yield_no_email_signal(email: str):
    profile = profile_prospect(email=email)
    assert profile.country_code is None
    assert profile.confidence == 0.0
    # But we should still record that we looked
    assert any("generic" in s for s in profile.signals)


def test_generic_tld_email_with_country_column_resolves():
    """If email is .com but CSV has country, country wins."""
    profile = profile_prospect(email="ceo@acme.com", country="Germany")
    assert profile.country_code == "DE"
    assert profile.language == "de"


# ---------------------------------------------------------------------------
# Conflicting signals — CSV beats phone beats email
# ---------------------------------------------------------------------------


def test_csv_country_outweighs_phone_alone():
    """CSV says India (0.6); phone says Algeria (0.4) → India wins."""
    profile = profile_prospect(country="India", phone="+21321111111")
    assert profile.country_code == "IN"
    # Only the IN vote contributes to confidence; the DZ vote is recorded but loses
    assert profile.confidence == pytest.approx(0.6)


def test_phone_plus_email_outweighs_csv_alone():
    """Phone (0.4) + email TLD (0.3) = 0.7 > CSV alone (0.6) when they agree."""
    profile = profile_prospect(
        country="Wakanda",  # unknown — adds to signals but no vote
        phone="+21321111111",
        email="x@y.dz",
    )
    assert profile.country_code == "DZ"
    assert profile.confidence == pytest.approx(0.7)
    assert any("Wakanda" in s and "unknown" in s for s in profile.signals)


def test_two_agreeing_signals_saturate_to_one():
    profile = profile_prospect(country="France", email="a@b.fr")
    assert profile.country_code == "FR"
    # 0.6 + 0.3 = 0.9
    assert profile.confidence == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# Edge cases / no signal
# ---------------------------------------------------------------------------


def test_no_signals_returns_empty_profile():
    profile = profile_prospect()
    assert profile.country is None
    assert profile.country_code is None
    assert profile.language is None
    assert profile.confidence == 0.0
    assert profile.signals == []


def test_all_blank_strings_returns_empty_profile():
    profile = profile_prospect(email="", phone="", country="")
    assert profile.country_code is None
    assert profile.confidence == 0.0


def test_unprefixed_phone_is_ignored():
    """A 10-digit local number with no + or 00 prefix gives no phone signal."""
    profile = profile_prospect(phone="9876543210")
    assert profile.country_code is None


def test_email_without_at_sign_ignored():
    profile = profile_prospect(email="not-an-email")
    assert profile.country_code is None


def test_email_subdomain_tld_extracted_correctly():
    """sub.domain.dz still picks .dz."""
    profile = profile_prospect(email="ceo@sub.domain.dz")
    assert profile.country_code == "DZ"


def test_uk_cctld_resolves_to_gb():
    profile = profile_prospect(email="x@bbc.co.uk")
    assert profile.country_code == "GB"
    assert profile.language == "en"


def test_country_uk_alias_resolves_to_gb():
    assert profile_prospect(country="UK").country_code == "GB"
    assert profile_prospect(country="Great Britain").country_code == "GB"


def test_us_ca_calling_code_resolves_to_us_primary():
    """+1 is shared by US and Canada; we pick US as primary."""
    profile = profile_prospect(phone="+12125551234")
    assert profile.country_code == "US"


def test_unknown_country_string_recorded_but_no_vote():
    profile = profile_prospect(country="Atlantis")
    assert profile.country_code is None
    assert any("Atlantis" in s and "unknown" in s for s in profile.signals)


# ---------------------------------------------------------------------------
# profile_prospect_obj wrapper
# ---------------------------------------------------------------------------


def test_profile_prospect_obj_with_dict():
    profile = profile_prospect_obj({"email": "a@b.dz", "country": "Algeria"})
    assert profile.country_code == "DZ"


def test_profile_prospect_obj_with_object_like_prospect():
    p = SimpleNamespace(email="raj@example.in", phone="+919876543210", country="India")
    profile = profile_prospect_obj(p)
    assert profile.country_code == "IN"
    assert profile.confidence == 1.0


def test_profile_prospect_obj_missing_attrs_safe():
    """Object without all three attrs shouldn't blow up."""
    p = SimpleNamespace(email="a@b.dz")
    profile = profile_prospect_obj(p)
    assert profile.country_code == "DZ"


# ---------------------------------------------------------------------------
# apply_profile_to_prospect — model-write helper
# ---------------------------------------------------------------------------


def test_apply_profile_writes_derived_fields_and_fills_missing_country():
    p = SimpleNamespace(
        country=None,
        detected_country_code=None,
        search_locale=None,
    )
    profile = ProspectProfile(
        country="Algeria",
        country_code="DZ",
        search_locale="dz",
        language="fr",
    )
    apply_profile_to_prospect(p, profile)
    assert p.country == "Algeria"
    assert p.detected_country_code == "DZ"
    assert p.search_locale == "dz"


def test_apply_profile_does_not_overwrite_csv_country():
    """If the CSV provided 'Algerie' (raw), we keep it as-is — never overwrite."""
    p = SimpleNamespace(
        country="Algerie",
        detected_country_code=None,
        search_locale=None,
    )
    profile = ProspectProfile(
        country="Algeria",
        country_code="DZ",
        search_locale="dz",
        language="fr",
    )
    apply_profile_to_prospect(p, profile)
    assert p.country == "Algerie"  # preserved
    assert p.detected_country_code == "DZ"  # derived field set
    assert p.search_locale == "dz"


def test_apply_profile_with_empty_profile_writes_none_to_derived_fields():
    """No signals → derived fields explicitly cleared (None)."""
    p = SimpleNamespace(
        country=None,
        detected_country_code="STALE",
        search_locale="stale",
    )
    apply_profile_to_prospect(p, ProspectProfile())
    assert p.country is None
    assert p.detected_country_code is None
    assert p.search_locale is None


# ---------------------------------------------------------------------------
# language_for_country_code helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code,expected", [
    ("DZ", "fr"),
    ("dz", "fr"),     # case-insensitive
    ("IN", "en"),
    ("FR", "fr"),
    ("DE", "de"),
    ("JP", "ja"),
    ("XX", None),     # unknown
    ("", None),
    (None, None),
])
def test_language_for_country_code(code, expected):
    assert language_for_country_code(code) == expected
