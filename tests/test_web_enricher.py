"""Tests for the rewritten web enricher — Phase 5.

Two surfaces:
    1. ``build_search_plan`` — pure function, no I/O, easy to assert on.
    2. ``enrich_prospect`` / ``enrich_batch`` — live + mock paths via httpx
       MockTransport so we can assert the exact JSON sent to Serper.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from backend.pipeline import web_enricher as we


# ---------------------------------------------------------------------------
# build_search_plan — geo-aware queries
# ---------------------------------------------------------------------------


def test_plan_groupe_rymm_algeria_scenario():
    """The headline scenario: GROUPE RYMM, Algeria, French language.

    Queries should NOT contain 'India' or 'manufacturer'; geo params should
    use Algeria's locale/language."""
    plan = we.build_search_plan(
        {
            "name": "Karim Belaid",
            "company_name": "GROUPE RYMM",
            "country": "Algeria",
            "search_locale": "dz",
            "language": "fr",
        }
    )
    assert plan.company_query == '"GROUPE RYMM" Algeria'
    assert plan.person_query == 'site:linkedin.com "Karim Belaid" "GROUPE RYMM"'
    assert plan.serper_params == {"gl": "dz", "hl": "fr"}
    # The legacy hardcodes that this rewrite is fixing — make sure they're gone
    assert "India" not in plan.company_query
    assert "manufacturer" not in plan.company_query


def test_plan_indian_prospect_uses_indian_locale():
    """Regression: India prospects still get the right geo signal — but it
    comes from their profile, not a hardcoded default."""
    plan = we.build_search_plan(
        {
            "name": "Raj Patel",
            "company_name": "ABC Plastics",
            "country": "India",
            "search_locale": "in",
            "language": "en",
        }
    )
    assert plan.company_query == '"ABC Plastics" India'
    assert plan.person_query == 'site:linkedin.com "Raj Patel" "ABC Plastics"'
    assert plan.serper_params == {"gl": "in", "hl": "en"}


def test_plan_no_profile_falls_back_to_city():
    """Without country we use city for geo grounding (still better than nothing)."""
    plan = we.build_search_plan(
        {
            "name": "Alice",
            "company_name": "Acme",
            "city": "Pune",
        }
    )
    assert plan.company_query == '"Acme" Pune'
    assert plan.serper_params == {}  # no locale/language → no geo params


def test_plan_no_geo_at_all():
    """Bare minimum: just company name → quoted, no geo."""
    plan = we.build_search_plan({"name": "X", "company_name": "Foo Co"})
    assert plan.company_query == '"Foo Co"'
    assert plan.serper_params == {}


def test_plan_skips_company_query_when_scraped_data_present():
    """Phase 4 already gave us company data — don't burn Serper on it."""
    plan = we.build_search_plan(
        {
            "name": "Karim",
            "company_name": "GROUPE RYMM",
            "country": "Algeria",
            "search_locale": "dz",
            "language": "fr",
            "has_scraped_data": True,
        }
    )
    assert plan.company_query is None
    # Person query still runs — LinkedIn isn't on the company website
    assert plan.person_query == 'site:linkedin.com "Karim" "GROUPE RYMM"'
    assert plan.serper_params == {"gl": "dz", "hl": "fr"}


def test_plan_skips_person_query_when_no_name():
    plan = we.build_search_plan(
        {"company_name": "GROUPE RYMM", "country": "Algeria"}
    )
    assert plan.company_query == '"GROUPE RYMM" Algeria'
    assert plan.person_query is None


def test_plan_returns_none_company_query_when_no_company_or_geo():
    """Email-only prospects: no useful company query to issue."""
    plan = we.build_search_plan({"name": "Alice"})
    assert plan.company_query is None
    assert plan.person_query == 'site:linkedin.com "Alice"'


def test_plan_strips_inner_quotes_from_company_name():
    """Company names with embedded quotes break Serper's exact-match — strip them."""
    plan = we.build_search_plan(
        {"company_name": 'ACME "International" Ltd', "country": "FR"}
    )
    assert plan.company_query == '"ACME International Ltd" FR'


def test_plan_handles_blank_strings_like_missing():
    plan = we.build_search_plan(
        {
            "name": "  ",
            "company_name": "",
            "country": "  ",
            "city": "Pune",
        }
    )
    assert plan.company_query == "Pune"  # only city left
    assert plan.person_query is None     # blank name == no person search


def test_plan_country_takes_precedence_over_city():
    """When both are present, country is the more stable geo signal."""
    plan = we.build_search_plan(
        {"company_name": "Acme", "country": "France", "city": "Paris"}
    )
    assert plan.company_query == '"Acme" France'


def test_plan_lowercases_locale_and_language():
    plan = we.build_search_plan(
        {"name": "X", "company_name": "Y", "search_locale": "DZ", "language": "FR"}
    )
    assert plan.serper_params == {"gl": "dz", "hl": "fr"}


# ---------------------------------------------------------------------------
# Mock-mode results — honor the plan
# ---------------------------------------------------------------------------


def test_mock_mode_returns_results_for_basic_prospect():
    """Backward-compat: original test shape still works, returns synthetic data."""
    out = asyncio.run(
        we.enrich_batch(
            [{"email": "x@y.com", "name": "X", "company_name": "Foo Co", "city": "Pune"}]
        )
    )
    assert "x@y.com" in out
    assert out["x@y.com"].company_results
    assert out["x@y.com"].person_results
    # The plan should be attached for observability
    assert out["x@y.com"].plan is not None
    assert "India" not in (out["x@y.com"].plan.company_query or "")


def test_mock_mode_skips_company_results_when_scraped():
    """If has_scraped_data is set, mock returns empty company_results."""
    out = asyncio.run(
        we.enrich_batch(
            [
                {
                    "email": "x@y.com",
                    "name": "X",
                    "company_name": "Foo Co",
                    "country": "Algeria",
                    "search_locale": "dz",
                    "language": "fr",
                    "has_scraped_data": True,
                }
            ]
        )
    )
    snippets = out["x@y.com"]
    assert snippets.company_results == []
    # Person results still present (LinkedIn isn't on the company site)
    assert snippets.person_results
    assert snippets.plan.company_query is None


# ---------------------------------------------------------------------------
# Live path — mocked Serper transport, asserts exact JSON sent
# ---------------------------------------------------------------------------


def _capture_handler(captured: list[dict]):
    """Records every Serper request and returns 2 organic results each."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        captured.append(body)
        return httpx.Response(
            200,
            json={
                "organic": [
                    {"title": f"R1 for {body['q']}", "link": "https://example.com", "snippet": "x"},
                    {"title": f"R2 for {body['q']}", "link": "https://linkedin.com/in/x", "snippet": "y"},
                ]
            },
        )

    return handler


def test_live_path_sends_correct_json_to_serper():
    """The Algeria prospect should produce two queries with the right gl/hl."""
    captured: list[dict] = []
    client = httpx.AsyncClient(transport=httpx.MockTransport(_capture_handler(captured)))

    async def run():
        return await we.enrich_prospect(
            client,
            api_key="fake-key",
            prospect={
                "email": "infos@itp.dz",
                "name": "Karim Belaid",
                "company_name": "GROUPE RYMM",
                "country": "Algeria",
                "search_locale": "dz",
                "language": "fr",
            },
        )

    snippets = asyncio.run(run())
    asyncio.run(client.aclose())

    # Both queries fired
    assert len(captured) == 2
    queries = sorted(c["q"] for c in captured)
    assert queries == [
        '"GROUPE RYMM" Algeria',
        'site:linkedin.com "Karim Belaid" "GROUPE RYMM"',
    ]
    # Geo params on every request
    for call in captured:
        assert call["gl"] == "dz"
        assert call["hl"] == "fr"
        assert call["num"] == 5
    # And we got the snippets back
    assert snippets.company_results
    assert snippets.person_results


def test_live_path_skips_company_query_when_scraped(monkeypatch: pytest.MonkeyPatch):
    """has_scraped_data=True → only the person query hits Serper."""
    captured: list[dict] = []
    client = httpx.AsyncClient(transport=httpx.MockTransport(_capture_handler(captured)))

    async def run():
        return await we.enrich_prospect(
            client,
            api_key="fake-key",
            prospect={
                "email": "infos@itp.dz",
                "name": "Karim Belaid",
                "company_name": "GROUPE RYMM",
                "country": "Algeria",
                "search_locale": "dz",
                "language": "fr",
                "has_scraped_data": True,
            },
        )

    snippets = asyncio.run(run())
    asyncio.run(client.aclose())
    assert len(captured) == 1
    assert captured[0]["q"] == 'site:linkedin.com "Karim Belaid" "GROUPE RYMM"'
    assert snippets.company_results == []
    assert snippets.person_results       # person results came through


def test_live_path_no_geo_params_when_profile_absent():
    """Without locale/language we send NO gl/hl — better to let Serper decide
    than to hardcode a default."""
    captured: list[dict] = []
    client = httpx.AsyncClient(transport=httpx.MockTransport(_capture_handler(captured)))

    async def run():
        return await we.enrich_prospect(
            client,
            api_key="fake-key",
            prospect={
                "email": "x@y.com",
                "name": "Alice",
                "company_name": "Acme",
                "city": "Pune",
            },
        )

    asyncio.run(run())
    asyncio.run(client.aclose())
    for call in captured:
        assert "gl" not in call
        assert "hl" not in call


def test_live_path_handles_serper_error_returns_empty_snippets():
    """Serper bombs → we log and return empty results, never raise."""

    def boom(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="serper down")

    client = httpx.AsyncClient(transport=httpx.MockTransport(boom))

    async def run():
        return await we.enrich_prospect(
            client,
            api_key="fake-key",
            prospect={
                "email": "x@y.com",
                "name": "Alice",
                "company_name": "Acme",
                "country": "France",
                "search_locale": "fr",
                "language": "fr",
            },
        )

    snippets = asyncio.run(run())
    asyncio.run(client.aclose())
    assert snippets.company_results == []
    assert snippets.person_results == []
    # Plan still attached — useful for the pipeline-logs UI
    assert snippets.plan is not None
    assert snippets.plan.company_query == '"Acme" France'


# ---------------------------------------------------------------------------
# enrich_batch — backward-compat smoke
# ---------------------------------------------------------------------------


def test_enrich_batch_keys_results_by_email():
    out = asyncio.run(
        we.enrich_batch(
            [
                {"email": "a@b.com", "name": "A", "company_name": "Co A", "country": "France",
                 "search_locale": "fr", "language": "fr"},
                {"email": "c@d.com", "name": "C", "company_name": "Co C", "country": "Algeria",
                 "search_locale": "dz", "language": "fr", "has_scraped_data": True},
            ]
        )
    )
    assert set(out.keys()) == {"a@b.com", "c@d.com"}
    # First prospect: both queries planned, mock fills both
    assert out["a@b.com"].plan.company_query == '"Co A" France'
    assert out["a@b.com"].company_results
    # Second prospect: company query skipped (has_scraped_data)
    assert out["c@d.com"].plan.company_query is None
    assert out["c@d.com"].company_results == []
    assert out["c@d.com"].person_results
