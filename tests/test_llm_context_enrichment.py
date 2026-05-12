"""Tests for the multi-source LLM context enrichment — Phase 6.

Three surfaces:
    1. Prompt builder — profile + scrape + snippets all surface in the prompt.
    2. Mock extractor — honors source priority (scraped > snippets) so
       mock-mode pipelines exercise the new code path.
    3. ``to_db_fields`` — full data chain stored in ``enrichment_raw``.
"""

from __future__ import annotations

import asyncio

import pytest

from backend.pipeline import llm_extractor as le


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
TAXONOMY = ["Plastics & Polymers", "Packaging", "Industrial Machinery", "Unknown"]


def _algeria_prospect() -> dict:
    """The headline scenario — full Phase 1 + Phase 2 data."""
    return {
        "email": "infos@itp.dz",
        "name": "Karim Belaid",
        "company_name": "GROUPE RYMM",
        "city": "Algiers",
        "state": None,
        "country": "Algeria",
        "detected_country_code": "DZ",
        "language": "fr",
        "notes": "Met at Plastics expo",
        "website_csv": "http://www.grouperymm.com",
    }


def _algeria_scrape() -> dict:
    """Synthetic ScrapedSite-like dict for GROUPE RYMM."""
    return {
        "url": "https://www.grouperymm.com",
        "final_url": "https://www.grouperymm.com",
        "status": "ok",
        "title": "GROUPE RYMM | Emballage Plastique",
        "description": "Leader algérien de l'emballage plastique injecté depuis 1995.",
        "text": "Fondé en 1995, GROUPE RYMM fabrique des emballages plastiques sur mesure...",
        "about_url": "https://www.grouperymm.com/qui-sommes-nous",
        "about_text": "200 employés répartis sur 3 sites en Algérie...",
        "social_links": {
            "linkedin": "https://www.linkedin.com/company/groupe-rymm",
            "facebook": "https://facebook.com/grouperymm",
        },
        "status_code": 200,
        "error": None,
    }


def _serper_snippets() -> dict:
    return {
        "company_results": [
            {
                "title": "GROUPE RYMM",
                "link": "https://www.directory.dz/groupe-rymm",
                "snippet": "Plastics manufacturer in Algiers.",
            }
        ],
        "person_results": [
            {
                "title": "Karim Belaid",
                "link": "https://www.linkedin.com/in/karim-belaid",
                "snippet": "CEO at GROUPE RYMM.",
            }
        ],
    }


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------


def test_prompt_includes_full_prospect_context():
    """Profile fields (country, language, notes) all appear in the prompt."""
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=None,
        snippets=None,
        taxonomy=TAXONOMY,
    )
    assert "Karim Belaid" in prompt
    assert "GROUPE RYMM" in prompt
    assert "Algeria" in prompt
    assert "[DZ]" in prompt
    assert "Likely working language: fr" in prompt
    assert "domain: itp.dz" in prompt
    assert "Met at Plastics expo" in prompt
    assert "http://www.grouperymm.com" in prompt   # CSV-supplied website


def test_prompt_includes_scraped_section_when_present():
    """Scraped homepage + about + socials all surface in PRIMARY SOURCE."""
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=_algeria_scrape(),
        snippets=None,
        taxonomy=TAXONOMY,
    )
    assert "PRIMARY SOURCE" in prompt
    assert "GROUPE RYMM | Emballage Plastique" in prompt
    assert "Leader algérien" in prompt
    assert "Fondé en 1995" in prompt
    assert "200 employés" in prompt
    # Socials
    assert "linkedin.com/company/groupe-rymm" in prompt
    assert "facebook.com/grouperymm" in prompt


def test_prompt_marks_scrape_unavailable_when_status_not_ok():
    """If the scrape failed we tell the LLM so it knows what's missing."""
    failed_scrape = {"status": "http_error", "error": "404"}
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=failed_scrape,
        snippets=None,
        taxonomy=TAXONOMY,
    )
    assert "PRIMARY SOURCE" in prompt
    assert "NOT AVAILABLE" in prompt
    assert "http_error" in prompt


def test_prompt_marks_scrape_not_provided_when_none():
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=None,
        snippets=None,
        taxonomy=TAXONOMY,
    )
    assert "NOT PROVIDED" in prompt


def test_prompt_marks_snippets_as_supplementary():
    """The framing of snippets must explicitly say 'supplementary, not override'."""
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=_algeria_scrape(),
        snippets=_serper_snippets(),
        taxonomy=TAXONOMY,
    )
    assert "SUPPLEMENTARY" in prompt
    assert "fill gaps" in prompt
    assert "linkedin.com/in/karim-belaid" in prompt


def test_prompt_handles_empty_snippets_gracefully():
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=_algeria_scrape(),
        snippets={"company_results": [], "person_results": []},
        taxonomy=TAXONOMY,
    )
    assert "(none — website data is the only source)" in prompt


def test_prompt_emits_taxonomy_and_json_schema():
    prompt = le._build_user_prompt(
        prospect=_algeria_prospect(),
        scraped=None,
        snippets=None,
        taxonomy=TAXONOMY,
    )
    for industry in TAXONOMY:
        assert industry in prompt
    assert '"company_website"' in prompt
    assert '"industry_confidence"' in prompt


def test_prompt_optional_fields_omitted_when_absent():
    """Missing fields should produce a minimal prompt — no 'None' string leakage."""
    minimal = {"email": "x@y.com", "company_name": "Acme"}
    prompt = le._build_user_prompt(
        prospect=minimal, scraped=None, snippets=None, taxonomy=TAXONOMY
    )
    # No 'language' line because language wasn't supplied
    assert "Likely working language" not in prompt
    # No CSV notes line
    assert "CSV notes" not in prompt
    # Country falls back to '?'
    assert "Location: ?, ?, ?" in prompt


# ---------------------------------------------------------------------------
# Mock extractor — honors source priority
# ---------------------------------------------------------------------------


def test_mock_extract_uses_scraped_website_over_serper():
    """Scraped final_url wins over any URL in Serper snippets."""
    snippets = {
        "company_results": [
            {"title": "x", "link": "https://other-site.com", "snippet": "x"}
        ],
        "person_results": [],
    }
    out = le._mock_extract(
        company_name="GROUPE RYMM",
        snippets=snippets,
        scraped=_algeria_scrape(),
        taxonomy=TAXONOMY,
    )
    assert out.company_website == "https://www.grouperymm.com"


def test_mock_extract_uses_scraped_description_over_snippet():
    out = le._mock_extract(
        company_name="GROUPE RYMM",
        snippets=_serper_snippets(),
        scraped=_algeria_scrape(),
        taxonomy=TAXONOMY,
    )
    assert "algérien" in (out.company_description or "")


def test_mock_extract_uses_scraped_linkedin_over_serper():
    """Company LinkedIn from social_links beats anything Serper found."""
    snippets = {
        "company_results": [
            {"link": "https://www.linkedin.com/company/wrong-co", "snippet": "x"}
        ],
        "person_results": [],
    }
    out = le._mock_extract(
        company_name="GROUPE RYMM",
        snippets=snippets,
        scraped=_algeria_scrape(),
        taxonomy=TAXONOMY,
    )
    assert out.company_linkedin == "https://www.linkedin.com/company/groupe-rymm"


def test_mock_extract_falls_back_to_snippets_when_no_scrape():
    """Backward compat: pre-Phase-4 prospects (no scrape) still get reasonable data."""
    out = le._mock_extract(
        company_name="Foo Co",
        snippets=_serper_snippets(),
        scraped=None,
        taxonomy=TAXONOMY,
    )
    assert out.company_website == "https://www.directory.dz/groupe-rymm"
    assert out.company_description == "Plastics manufacturer in Algiers."
    # Person LinkedIn always comes from Serper (scraper doesn't see it)
    assert out.person_linkedin == "https://www.linkedin.com/in/karim-belaid"


def test_mock_extract_higher_confidence_when_scrape_present():
    """Confidence should be biased higher when we have first-party data."""
    with_scrape = le._mock_extract(
        company_name="X", snippets=_serper_snippets(),
        scraped=_algeria_scrape(), taxonomy=TAXONOMY,
    )
    without_scrape = le._mock_extract(
        company_name="X", snippets=_serper_snippets(),
        scraped=None, taxonomy=TAXONOMY,
    )
    # Lower bound of the with-scrape range > lower bound of without-scrape range
    assert with_scrape.industry_confidence >= 0.7 - 1e-9
    assert without_scrape.industry_confidence <= 0.85 + 1e-9


def test_mock_extract_handles_failed_scrape_gracefully():
    """Scrape returned an error → fall back to Serper-only behavior."""
    out = le._mock_extract(
        company_name="X",
        snippets=_serper_snippets(),
        scraped={"status": "http_error", "error": "404"},
        taxonomy=TAXONOMY,
    )
    # Should not pick up scraped fields when status != ok
    assert out.company_website != "https://www.grouperymm.com"


# ---------------------------------------------------------------------------
# extract_fields end-to-end (mock mode)
# ---------------------------------------------------------------------------


def test_extract_fields_full_context_mock_mode(monkeypatch: pytest.MonkeyPatch):
    """The headline scenario through the public API.

    Forces mock mode (OPENAI_API_KEY="") so the test is deterministic regardless
    of whether the dev .env has a real key configured.
    """
    from backend.config import get_settings
    settings = get_settings()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "", raising=False)
    out = asyncio.run(
        le.extract_fields(
            _algeria_prospect(),
            _serper_snippets(),
            TAXONOMY,
            scraped=_algeria_scrape(),
        )
    )
    assert out.company_website == "https://www.grouperymm.com"
    assert "algérien" in (out.company_description or "")
    assert out.company_linkedin == "https://www.linkedin.com/company/groupe-rymm"
    assert out.person_linkedin == "https://www.linkedin.com/in/karim-belaid"
    assert out.industry in TAXONOMY


def test_extract_fields_backward_compat_three_arg_call():
    """Existing callers (orchestrator, old test) using positional 3-arg form still work."""
    out = asyncio.run(
        le.extract_fields(
            {"name": "X", "company_name": "Foo Co", "city": "Pune", "state": "MH"},
            _serper_snippets(),
            TAXONOMY,
        )
    )
    assert out.industry in TAXONOMY


# ---------------------------------------------------------------------------
# to_db_fields — full data chain in enrichment_raw
# ---------------------------------------------------------------------------


def test_to_db_fields_stores_full_data_chain():
    extracted = le.ExtractedFields(
        company_website="https://www.grouperymm.com",
        company_description="x",
        industry="Packaging",
        industry_confidence=0.9,
        sub_category=None,
        company_size="medium",
        person_linkedin=None,
        company_linkedin="https://www.linkedin.com/company/groupe-rymm",
        relevance_score=0.7,
    )
    snippets = _serper_snippets()
    scraped = _algeria_scrape()
    profile = {"country": "Algeria", "country_code": "DZ", "search_locale": "dz", "language": "fr"}
    plan = {"company_query": None, "person_query": '"Karim Belaid"', "serper_params": {"gl": "dz"}}

    out = le.to_db_fields(
        extracted, snippets, scraped=scraped, profile=profile, search_plan=plan
    )
    raw = out["enrichment_raw"]
    assert raw["snippets"] == snippets
    assert raw["scraped"] == scraped
    assert raw["profile"] == profile
    assert raw["search_plan"] == plan
    # Top-level fields populated as before
    assert out["company_website"] == "https://www.grouperymm.com"
    assert out["industry"] == "Packaging"
    assert out["enriched_at"] is not None


def test_to_db_fields_backward_compat_two_arg_call():
    """Orchestrator's ``to_db_fields(extracted, snippets)`` still works."""
    extracted = le.ExtractedFields(
        company_website=None, company_description=None,
        industry="Unknown", industry_confidence=0.5,
        sub_category=None, company_size="unknown",
        person_linkedin=None, company_linkedin=None,
        relevance_score=0.0,
    )
    out = le.to_db_fields(extracted, _serper_snippets())
    assert out["enrichment_raw"]["snippets"] == _serper_snippets()
    # Phase-6-only keys absent when not passed
    assert "scraped" not in out["enrichment_raw"]
    assert "profile" not in out["enrichment_raw"]
    assert "search_plan" not in out["enrichment_raw"]


def test_to_db_fields_handles_no_snippets():
    """When the company query was skipped (Phase 5), snippets may be empty."""
    extracted = le.ExtractedFields(
        company_website="https://x.com", company_description=None,
        industry="Unknown", industry_confidence=0.0,
        sub_category=None, company_size="unknown",
        person_linkedin=None, company_linkedin=None,
        relevance_score=0.0,
    )
    out = le.to_db_fields(extracted, None, scraped=_algeria_scrape())
    assert out["enrichment_raw"]["snippets"] == {}
    assert out["enrichment_raw"]["scraped"]["status"] == "ok"
