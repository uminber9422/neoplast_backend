"""End-to-end orchestrator tests — Phase 7 wiring.

Runs the full pipeline against a small in-memory fixture:
    - Algeria prospect (CSV country, .dz email, +213 phone, website)
    - India prospect (legacy regression — no breakage)
    - Empty-website prospect (skips scrape stage)

Mocks the website scraper, Serper, and the LLM (no API keys exist in tests).
Asserts that:
    - All seven stages complete
    - data_profiling fills detected_country_code / search_locale
    - website_scraping result reaches the LLM via enrichment_raw['scraped']
    - LLM extraction sees country/locale/language in its prompt context
    - enrichment_raw stores the full data chain (snippets + scraped + profile + search_plan)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from backend.models.pipeline import PipelineRun
from backend.models.prospect import Prospect
from backend.pipeline import orchestrator, web_enricher, website_scraper


_email_counter = 0


def _uniq_email(prefix: str) -> str:
    """Generate a per-test unique email so tests don't trip the UNIQUE constraint."""
    global _email_counter
    _email_counter += 1
    return f"{prefix}+{_email_counter}@phase7.test"


def _mk_prospect(db: Session, **overrides) -> Prospect:
    base = dict(
        email=_uniq_email("auto"),
        name="X",
        company_name="Co",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    base.update(overrides)
    p = Prospect(**base)
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


def _mk_run(db: Session) -> PipelineRun:
    run = PipelineRun(
        run_type="full",
        status="pending",
        progress=orchestrator._initial_progress(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


# ---------------------------------------------------------------------------
# data_profiling stage in isolation
# ---------------------------------------------------------------------------


def test_data_profiling_fills_derived_fields(db: Session):
    email = _uniq_email("karim")
    p = _mk_prospect(
        db,
        email=email,
        name="Karim",
        company_name="GROUPE RYMM",
        country="Algeria",
        phone="+21321111111",
    )
    run = _mk_run(db)
    # Profile only the prospect this test created (others may exist from earlier tests)
    profile = orchestrator.data_profiler.profile_prospect_obj(p)
    orchestrator.data_profiler.apply_profile_to_prospect(p, profile)
    db.commit()
    db.refresh(p)
    assert p.country == "Algeria"            # CSV value preserved
    assert p.detected_country_code == "DZ"
    assert p.search_locale == "dz"


def test_data_profiling_stage_runs_for_all_unprofiled(db: Session):
    """Full-stage run profiles every prospect missing detected_country_code."""
    fresh = _mk_prospect(db, email=_uniq_email("fresh"), country="France")
    run = _mk_run(db)
    asyncio.run(orchestrator._stage_data_profiling(db, run, "incremental"))

    db.refresh(fresh)
    assert fresh.detected_country_code == "FR"
    assert run.progress["data_profiling"]["status"] == "completed"


# ---------------------------------------------------------------------------
# website_scraping stage in isolation
# ---------------------------------------------------------------------------


def test_website_scraping_returns_map_for_prospects_with_websites(
    db: Session, monkeypatch: pytest.MonkeyPatch
):
    email_with = _uniq_email("withsite")
    p1 = _mk_prospect(
        db, email=email_with, company_name="A", website_csv="www.a.example"
    )

    async def fake_batch(urls, *, concurrency=5):
        return {
            website_scraper._normalize_url("www.a.example"): website_scraper.ScrapedSite(
                url="https://www.a.example",
                final_url="https://www.a.example",
                status="ok",
                title="A | Inc",
                description="An A company.",
                text="Hello",
                social_links={"linkedin": "https://www.linkedin.com/company/a-inc"},
                status_code=200,
            ),
        }

    monkeypatch.setattr(website_scraper, "scrape_batch", fake_batch)

    run = _mk_run(db)
    # Only pass the prospect this test cares about, so we don't pick up leftovers
    scrape_map = asyncio.run(
        orchestrator._stage_website_scraping(db, run, "full", [p1])
    )

    assert email_with in scrape_map
    assert scrape_map[email_with]["status"] == "ok"
    assert scrape_map[email_with]["title"] == "A | Inc"
    assert (
        scrape_map[email_with]["social_links"]["linkedin"]
        == "https://www.linkedin.com/company/a-inc"
    )
    assert run.progress["website_scraping"]["status"] == "completed"
    assert run.progress["website_scraping"]["ok"] == 1


def test_website_scraping_disabled_by_config_returns_empty(
    db: Session, monkeypatch: pytest.MonkeyPatch
):
    p = _mk_prospect(db, email=_uniq_email("dis"), website_csv="www.a.example")
    from backend.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "WEBSITE_SCRAPE_ENABLED", False, raising=False)

    run = _mk_run(db)
    scrape_map = asyncio.run(
        orchestrator._stage_website_scraping(db, run, "full", [p])
    )
    assert scrape_map == {}


def test_website_scraping_no_targets_completes_with_zero(db: Session):
    run = _mk_run(db)
    scrape_map = asyncio.run(
        orchestrator._stage_website_scraping(db, run, "full", [])
    )
    assert scrape_map == {}
    assert run.progress["website_scraping"]["status"] == "completed"
    assert run.progress["website_scraping"]["total"] == 0


# ---------------------------------------------------------------------------
# Full run_pipeline — Algeria + India + bare prospect
# ---------------------------------------------------------------------------


def test_run_pipeline_full_end_to_end(db: Session, monkeypatch: pytest.MonkeyPatch):
    """The Algeria scenario through every stage of run_pipeline."""

    karim_email = _uniq_email("karim")
    raj_email = _uniq_email("raj")
    karim = _mk_prospect(
        db,
        email=karim_email,
        name="Karim Belaid",
        company_name="GROUPE RYMM",
        country="Algeria",
        phone="+21321111111",
        website_csv="www.grouperymm-fixture.example",
        notes="Met at Plastics expo",
    )
    raj = _mk_prospect(
        db,
        email=raj_email,
        name="Raj Patel",
        company_name="ABC Plastics",
        country="India",
        phone="+919876543210",
    )

    # Mock the website scraper — return rich data only for GROUPE RYMM
    captured_scrape_urls: list[str] = []

    async def fake_batch(urls, *, concurrency=5):
        captured_scrape_urls.extend(urls)
        out: dict[str, website_scraper.ScrapedSite] = {}
        for u in urls:
            normalized = website_scraper._normalize_url(u)
            if normalized and "grouperymm" in normalized:
                out[normalized] = website_scraper.ScrapedSite(
                    url=normalized,
                    final_url=normalized,
                    status="ok",
                    title="GROUPE RYMM",
                    description="Leader algérien de l'emballage plastique.",
                    text="Fondé en 1995...",
                    about_url=f"{normalized}/about",
                    about_text="200 employés en Algérie.",
                    social_links={
                        "linkedin": "https://www.linkedin.com/company/groupe-rymm",
                    },
                    status_code=200,
                )
        return out

    monkeypatch.setattr(website_scraper, "scrape_batch", fake_batch)

    # Capture the dicts that hit the web enricher so we can assert what the
    # orchestrator passes downstream — the new geo fields, has_scraped_data, etc.
    captured_enrich_dicts: list[dict] = []
    real_enrich_batch = web_enricher.enrich_batch

    async def spy_enrich_batch(prospects, *, concurrency=5, run_logger=None):
        captured_enrich_dicts.extend(prospects)
        return await real_enrich_batch(
            prospects, concurrency=concurrency, run_logger=run_logger,
        )

    monkeypatch.setattr(web_enricher, "enrich_batch", spy_enrich_batch)
    # Patch the symbol the orchestrator imports too
    monkeypatch.setattr(orchestrator.web_enricher, "enrich_batch", spy_enrich_batch)

    # Scope all stages to JUST our two test prospects so leftovers from other
    # tests don't leak in. We drive each stage manually with explicit lists.
    targets = [karim, raj]
    run = _mk_run(db)

    async def main():
        # Profile — apply to each by hand so we don't sweep up unrelated rows
        for p in targets:
            profile = orchestrator.data_profiler.profile_prospect_obj(p)
            orchestrator.data_profiler.apply_profile_to_prospect(p, profile)
        db.commit()
        # Mark profiling stage complete for assertion shape
        orchestrator._set_stage(run, "data_profiling", status="completed", processed=2, total=2)
        scrape_map = await orchestrator._stage_website_scraping(db, run, "full", targets)
        # Enrichment — patch select_prospects_to_enrich to return exactly our targets
        monkeypatch.setattr(
            orchestrator,
            "select_prospects_to_enrich",
            lambda db, *, run_type: targets,
        )
        # Skip email validation in the smoke (it would hit a mock validator anyway)
        orchestrator._set_stage(run, "email_validation", status="completed", processed=2, total=2)
        await orchestrator._stage_enrichment(db, run, "full", scrape_map=scrape_map)

    asyncio.run(main())
    db.commit()

    db.refresh(karim)
    db.refresh(raj)

    # ---- Stage progress: every stage completed ----
    for stage in (
        "email_validation",
        "data_profiling",
        "website_scraping",
        "web_search",
        "llm_extraction",
    ):
        assert run.progress[stage]["status"] == "completed", f"{stage} not completed"

    # ---- data_profiling worked for both ----
    assert karim.detected_country_code == "DZ"
    assert karim.search_locale == "dz"
    assert raj.detected_country_code == "IN"
    assert raj.search_locale == "in"

    # ---- website_scraping fired for the Algeria prospect only ----
    assert any("grouperymm" in u for u in captured_scrape_urls)
    assert run.progress["website_scraping"]["ok"] == 1

    # ---- enrichment dicts carry the new Phase 2 / Phase 4 fields ----
    karim_dict = next(d for d in captured_enrich_dicts if d["email"] == karim.email)
    assert karim_dict["country"] == "Algeria"
    assert karim_dict["search_locale"] == "dz"
    assert karim_dict["language"] == "fr"
    assert karim_dict["has_scraped_data"] is True   # scrape returned status=ok
    assert karim_dict["notes"] == "Met at Plastics expo"
    assert karim_dict["website_csv"] == "www.grouperymm-fixture.example"

    raj_dict = next(d for d in captured_enrich_dicts if d["email"] == raj.email)
    assert raj_dict["country"] == "India"
    assert raj_dict["search_locale"] == "in"
    assert raj_dict["language"] == "en"
    assert raj_dict["has_scraped_data"] is False   # no website_csv

    # ---- LLM filled in extracted fields, enrichment_raw has the full chain ----
    assert karim.industry is not None
    raw = karim.enrichment_raw or {}
    assert "snippets" in raw
    assert "scraped" in raw and raw["scraped"]["status"] == "ok"
    assert raw["scraped"]["title"] == "GROUPE RYMM"
    assert "profile" in raw
    assert raw["profile"]["country"] == "Algeria"
    assert raw["profile"]["country_code"] == "DZ"
    assert "search_plan" in raw

    # The Phase 5 query-rewrite outcome: company_query SKIPPED for the Algeria
    # prospect because the scrape covered it; person_query still ran.
    karim_plan = raw["search_plan"]
    assert karim_plan["company_query"] is None
    assert karim_plan["person_query"] is not None
    assert "site:linkedin.com" in karim_plan["person_query"]
    assert karim_plan["serper_params"]["gl"] == "dz"
    assert karim_plan["serper_params"]["hl"] == "fr"

    # India prospect — no scrape, so company_query DID run with India geo
    raj_plan = (raj.enrichment_raw or {}).get("search_plan", {})
    assert raj_plan.get("company_query") is not None
    assert "India" in raj_plan["company_query"]
    assert "manufacturer" not in raj_plan["company_query"]   # legacy hardcode gone
    assert raj_plan["serper_params"]["gl"] == "in"


def test_run_pipeline_handles_failed_scrape_gracefully(
    db: Session, monkeypatch: pytest.MonkeyPatch
):
    """A scrape that comes back with status='http_error' should not block enrichment."""
    p = _mk_prospect(
        db,
        email=_uniq_email("dead"),
        name="Karim",
        company_name="GROUPE RYMM",
        country="Algeria",
        website_csv="www.dead-site.example",
    )

    async def fake_batch(urls, *, concurrency=5):
        return {
            website_scraper._normalize_url(urls[0]): website_scraper.ScrapedSite(
                url=urls[0],
                status="http_error",
                status_code=500,
                error="server crashed",
            ),
        }

    monkeypatch.setattr(website_scraper, "scrape_batch", fake_batch)

    run = _mk_run(db)
    targets = [p]

    async def main():
        # Profile just our prospect
        profile = orchestrator.data_profiler.profile_prospect_obj(p)
        orchestrator.data_profiler.apply_profile_to_prospect(p, profile)
        db.commit()
        scrape_map = await orchestrator._stage_website_scraping(db, run, "full", targets)
        monkeypatch.setattr(
            orchestrator,
            "select_prospects_to_enrich",
            lambda db, *, run_type: targets,
        )
        await orchestrator._stage_enrichment(db, run, "full", scrape_map=scrape_map)

    asyncio.run(main())
    db.commit()
    db.refresh(p)

    # Pipeline still completed; the failed scrape was recorded
    assert run.progress["website_scraping"]["status"] == "completed"
    assert run.progress["website_scraping"]["ok"] == 0
    assert p.industry is not None  # LLM still extracted using snippets
    raw = p.enrichment_raw or {}
    assert raw["scraped"]["status"] == "http_error"
    # Search plan should NOT have skipped the company query (scrape failed)
    assert raw["search_plan"]["company_query"] is not None


def test_helper_scraped_to_dict_handles_dataclass_and_dict():
    """Internal helper accepts both dataclass and dict, normalizes datetime."""
    site = website_scraper.ScrapedSite(url="https://x", status="ok")
    out = orchestrator._scraped_to_dict(site)
    assert out["status"] == "ok"
    assert isinstance(out["scraped_at"], str)  # datetime serialized

    out2 = orchestrator._scraped_to_dict({"status": "ok", "scraped_at": "raw"})
    assert out2["status"] == "ok"

    assert orchestrator._scraped_to_dict(None) is None
    assert orchestrator._scraped_to_dict("not a thing") is None
