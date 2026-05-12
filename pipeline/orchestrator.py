"""Pipeline orchestrator.

Coordinates the seven stages of the lead enrichment pipeline:

    ingest → email_validation → data_profiling → website_scraping
           → web_search → llm_extraction → finalize

The two new stages (introduced in the Phase 2 / Phase 4 work) sit between
email validation and Serper search:

    - **data_profiling** derives country / locale / language from each
      prospect's signals (CSV column, email TLD, phone prefix). The result
      lands in ``country`` (when missing), ``detected_country_code``, and
      ``search_locale`` columns. Cheap, in-process, no network.
    - **website_scraping** fetches the prospect's own website (when supplied
      via the CSV ``website_csv`` column) and extracts title / description /
      about-page text / social links. The scrape is held in memory across
      stages so the LLM extractor can use it as PRIMARY ground truth without
      another round trip; the full data chain is also persisted into
      ``enrichment_raw`` for the pipeline-logs UI.

Incremental mode:
    - ``email_validation`` runs only for emails never validated, or older
      than ``STALE_THRESHOLD_DAYS``.
    - ``data_profiling`` runs for every prospect every run — it's cheap and
      idempotent; CSV-supplied ``country`` is never overwritten.
    - ``website_scraping`` + ``web_search`` + ``llm_extraction`` run only
      for prospects with NULL ``enriched_at``.
"""

from __future__ import annotations

import asyncio
import traceback
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.logging_setup import get_logger
from backend.models.database import SessionLocal
from backend.models.pipeline import PipelineRun
from backend.models.prospect import Prospect
from backend.pipeline import (
    data_profiler,
    email_validator,
    llm_extractor,
    web_enricher,
    website_scraper,
)
from backend.pipeline.run_logger import RunLogger
from backend.utils.industry_taxonomy import get_taxonomy

log = get_logger(__name__)

STAGES = (
    "ingest",
    "email_validation",
    "data_profiling",
    "website_scraping",
    "web_search",
    "llm_extraction",
    "finalize",
)


def _initial_progress() -> dict[str, dict[str, Any]]:
    return {stage: {"status": "pending"} for stage in STAGES}


def _set_stage(run: PipelineRun, stage: str, **fields: Any) -> None:
    progress = dict(run.progress or _initial_progress())
    stage_data = dict(progress.get(stage, {}))
    stage_data.update(fields)
    progress[stage] = stage_data
    run.progress = progress
    run.current_step = stage


def _add_error(run: PipelineRun, stage: str, message: str) -> None:
    errors = list(run.error_log or [])
    errors.append(
        {
            "stage": stage,
            "ts": datetime.now(timezone.utc).isoformat(),
            "message": message[:1000],
        }
    )
    run.error_log = errors
    run.errors = (run.errors or 0) + 1


def _commit(db: Session) -> None:
    """Commit, swallowing errors so an in-flight commit failure doesn't kill the run."""
    try:
        db.commit()
    except Exception as exc:  # noqa: BLE001
        log.error("pipeline_commit_failed", error=str(exc))
        db.rollback()


def select_prospects_to_validate(db: Session, *, run_type: str) -> list[Prospect]:
    """Return prospects needing (re-)validation per PRD §5.2."""
    settings = get_settings()
    threshold = datetime.now(timezone.utc) - timedelta(days=settings.STALE_THRESHOLD_DAYS)
    query = db.query(Prospect)
    if run_type == "incremental":
        query = query.filter(
            or_(
                Prospect.email_validated_at.is_(None),
                Prospect.email_validated_at < threshold,
            )
        )
    return query.all()


def select_prospects_to_enrich(db: Session, *, run_type: str) -> list[Prospect]:
    """Return prospects needing web search + LLM extraction."""
    query = db.query(Prospect)
    if run_type == "incremental":
        query = query.filter(Prospect.enriched_at.is_(None))
    return query.all()


def select_prospects_to_profile(db: Session, *, run_type: str) -> list[Prospect]:
    """Return prospects needing geo profiling.

    For incremental runs we only profile prospects that have never been
    profiled (``detected_country_code IS NULL``). For full runs we re-profile
    everyone — cheap and CSV ``country`` is never overwritten by the helper.
    """
    query = db.query(Prospect)
    if run_type == "incremental":
        query = query.filter(Prospect.detected_country_code.is_(None))
    return query.all()


def _scraped_to_dict(scraped: Any) -> dict | None:
    """Convert a ScrapedSite dataclass (or already-a-dict) to a JSON-friendly dict."""
    if scraped is None:
        return None
    if is_dataclass(scraped) and not isinstance(scraped, type):
        d = asdict(scraped)
    elif isinstance(scraped, dict):
        d = dict(scraped)
    else:
        return None
    # ``scraped_at`` is a datetime — make it JSON-serializable for enrichment_raw.
    ts = d.get("scraped_at")
    if isinstance(ts, datetime):
        d["scraped_at"] = ts.isoformat()
    return d


def _profile_to_dict(profile: data_profiler.ProspectProfile) -> dict:
    return {
        "country": profile.country,
        "country_code": profile.country_code,
        "search_locale": profile.search_locale,
        "language": profile.language,
        "signals": list(profile.signals),
        "confidence": profile.confidence,
    }


async def _stage_email_validation(
    db: Session,
    run: PipelineRun,
    run_type: str,
    rl: RunLogger | None = None,
) -> None:
    settings = get_settings()
    rows = select_prospects_to_validate(db, run_type=run_type)
    total = len(rows)
    if rl:
        rl.stage_start("email_validation", total=total, run_type=run_type)
    _set_stage(run, "email_validation", status="running", processed=0, total=total, pct=0.0)
    _commit(db)

    if not rows:
        if rl:
            rl.stage_end("email_validation", processed=0, total=0, message="No emails to validate")
        _set_stage(run, "email_validation", status="completed", processed=0, total=0)
        _commit(db)
        return

    batch_size = 50
    processed = 0
    skipped = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        emails = [r.email for r in batch]
        try:
            results = await email_validator.validate_emails(emails, run_logger=rl)
        except Exception as exc:  # noqa: BLE001
            _add_error(run, "email_validation", f"batch {i}: {exc}")
            if rl:
                rl.error("email_validation", f"Batch {i} failed: {exc}")
            log.error("email_batch_failed", index=i, error=str(exc))
            continue

        for prospect in batch:
            res = results.get(prospect.email)
            if res is None:
                skipped += 1
                continue
            for k, v in email_validator.to_db_fields(res).items():
                setattr(prospect, k, v)
            run.emails_validated = (run.emails_validated or 0) + 1
        processed += len(batch)
        _set_stage(
            run,
            "email_validation",
            status="running",
            processed=processed,
            total=total,
            pct=round(processed / max(total, 1) * 100, 1),
        )
        _commit(db)

    run.emails_skipped = skipped
    if rl:
        rl.stage_end("email_validation", processed=processed, total=total, skipped=skipped)
    _set_stage(
        run,
        "email_validation",
        status="completed",
        processed=processed,
        total=total,
        pct=100.0,
    )
    _commit(db)


async def _stage_data_profiling(
    db: Session,
    run: PipelineRun,
    run_type: str,
    rl: RunLogger | None = None,
) -> None:
    """Derive country / locale / language for every targeted prospect.

    Pure in-process work: no network, no LLM. Handles thousands of prospects
    in milliseconds. Updates ``country`` (only when missing), plus the always-
    derived ``detected_country_code`` and ``search_locale`` columns.
    """
    rows = select_prospects_to_profile(db, run_type=run_type)
    total = len(rows)
    if rl:
        rl.stage_start("data_profiling", total=total, run_type=run_type)
    _set_stage(run, "data_profiling", status="running", processed=0, total=total, pct=0.0)
    _commit(db)

    if not rows:
        if rl:
            rl.stage_end("data_profiling", processed=0, total=0)
        _set_stage(run, "data_profiling", status="completed", processed=0, total=0)
        _commit(db)
        return

    processed = 0
    profiled = 0
    for prospect in rows:
        try:
            profile = data_profiler.profile_prospect_obj(prospect)
            data_profiler.apply_profile_to_prospect(prospect, profile)
            if profile.country_code is not None:
                profiled += 1
                if rl:
                    rl.log(
                        "geo_profile_resolved",
                        stage="data_profiling",
                        email=prospect.email,
                        country=profile.country,
                        country_code=profile.country_code,
                        search_locale=profile.search_locale,
                        language=profile.language,
                        confidence=profile.confidence,
                    )
        except Exception as exc:  # noqa: BLE001
            _add_error(run, "data_profiling", f"{prospect.email}: {exc}")
            if rl:
                rl.error("data_profiling", f"{prospect.email}: {exc}", email=prospect.email)
            log.error("data_profiling_failed", email=prospect.email, error=str(exc))
        processed += 1
        # Tick every 100 rows to keep the progress UI lively without flooding commits
        if processed % 100 == 0:
            _set_stage(
                run,
                "data_profiling",
                status="running",
                processed=processed,
                total=total,
                pct=round(processed / max(total, 1) * 100, 1),
            )
            _commit(db)

    if rl:
        rl.stage_end("data_profiling", processed=processed, total=total, profiled=profiled)
    _set_stage(
        run,
        "data_profiling",
        status="completed",
        processed=processed,
        total=total,
        profiled=profiled,
        pct=100.0,
    )
    _commit(db)
    log.info("data_profiling_complete", processed=processed, profiled=profiled)


async def _stage_website_scraping(
    db: Session,
    run: PipelineRun,
    run_type: str,
    targets: list[Prospect],
    rl: RunLogger | None = None,
) -> dict[str, dict]:
    """Scrape the company website for prospects that supplied one in the CSV.

    Returns ``{email: scraped_site_dict}`` keyed by prospect email. The dict
    is held in memory and consumed by the LLM extraction stage downstream.
    Prospects without a CSV-supplied website are simply absent from the map.
    """
    settings = get_settings()
    if not getattr(settings, "WEBSITE_SCRAPE_ENABLED", True):
        log.info("website_scraping_disabled_by_config")
        if rl:
            rl.log("website_scraping_disabled", stage="website_scraping")
        _set_stage(run, "website_scraping", status="completed", processed=0, total=0)
        _commit(db)
        return {}

    candidates = [(p.email, p.website_csv) for p in targets if p.website_csv]
    total = len(candidates)
    if rl:
        rl.stage_start("website_scraping", total=total)
    _set_stage(run, "website_scraping", status="running", processed=0, total=total, pct=0.0)
    _commit(db)

    if not candidates:
        if rl:
            rl.stage_end("website_scraping", processed=0, total=0)
        _set_stage(run, "website_scraping", status="completed", processed=0, total=0)
        _commit(db)
        return {}

    # Batch-scrape with bounded concurrency — config knob lives in Settings.
    concurrency = int(getattr(settings, "WEBSITE_SCRAPE_CONCURRENCY", 5))
    batch_size = max(concurrency * 4, 20)
    results: dict[str, dict] = {}

    for i in range(0, total, batch_size):
        batch = candidates[i : i + batch_size]
        urls = [w for _, w in batch]
        try:
            batch_results = await website_scraper.scrape_batch(urls, concurrency=concurrency)
        except Exception as exc:  # noqa: BLE001
            _add_error(run, "website_scraping", f"batch {i}: {exc}")
            log.error("website_scraping_batch_failed", index=i, error=str(exc))
            batch_results = {}

        for email, raw_url in batch:
            normalized = website_scraper._normalize_url(raw_url) or (raw_url or "")
            scraped = batch_results.get(normalized)
            if scraped is not None:
                site_dict = _scraped_to_dict(scraped) or {}
                results[email] = site_dict
                if rl:
                    rl.log(
                        "website_scraped",
                        stage="website_scraping",
                        email=email,
                        url=raw_url,
                        final_url=site_dict.get("final_url"),
                        status=site_dict.get("status"),
                        status_code=site_dict.get("status_code"),
                        title=site_dict.get("title"),
                        description=site_dict.get("description"),
                        social_links=site_dict.get("social_links"),
                    )

        processed = min(i + len(batch), total)
        _set_stage(
            run,
            "website_scraping",
            status="running",
            processed=processed,
            total=total,
            pct=round(processed / max(total, 1) * 100, 1),
        )
        _commit(db)

    ok_count = sum(1 for v in results.values() if v.get("status") == "ok")
    if rl:
        rl.stage_end("website_scraping", processed=total, total=total, ok=ok_count)
    _set_stage(
        run,
        "website_scraping",
        status="completed",
        processed=total,
        total=total,
        ok=ok_count,
        pct=100.0,
    )
    _commit(db)
    log.info("website_scraping_complete", scraped=len(results), ok=ok_count)
    return results


async def _stage_enrichment(
    db: Session,
    run: PipelineRun,
    run_type: str,
    scrape_map: dict[str, dict] | None = None,
    rl: RunLogger | None = None,
) -> None:
    rows = select_prospects_to_enrich(db, run_type=run_type)
    total = len(rows)
    if rl:
        rl.stage_start("web_search", total=total, run_type=run_type)
    _set_stage(run, "web_search", status="running", processed=0, total=total, pct=0.0)
    _set_stage(run, "llm_extraction", status="pending", processed=0, total=total, pct=0.0)
    _commit(db)

    if not rows:
        if rl:
            rl.stage_end("web_search", processed=0, total=0)
            rl.stage_end("llm_extraction", processed=0, total=0)
        _set_stage(run, "web_search", status="completed", processed=0, total=0)
        _set_stage(run, "llm_extraction", status="completed", processed=0, total=0)
        _commit(db)
        return

    scrape_map = scrape_map or {}
    taxonomy = get_taxonomy(db)
    settings = get_settings()
    batch_size = settings.LLM_BATCH_SIZE

    web_processed = 0
    llm_processed = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        # Build a richer prospect dict for each prospect: profile fields from
        # Phase 2, scraped-data flag from Phase 4, plus the new CSV fields
        # (notes, country) added in Phase 1. Phase 5's adaptive query builder
        # consumes all of this.
        batch_dicts: list[dict] = []
        for p in batch:
            scraped = scrape_map.get(p.email)
            has_scraped = bool(scraped and scraped.get("status") == "ok")
            batch_dicts.append(
                {
                    "email": p.email,
                    "name": p.name,
                    "company_name": p.company_name,
                    "city": p.city,
                    "state": p.state,
                    # New fields wired in Phase 7:
                    "country": p.country,
                    "detected_country_code": p.detected_country_code,
                    "search_locale": p.search_locale,
                    "language": data_profiler.language_for_country_code(
                        p.detected_country_code
                    ),
                    "notes": p.notes,
                    "website_csv": p.website_csv,
                    "has_scraped_data": has_scraped,
                }
            )
        try:
            snippets_map = await web_enricher.enrich_batch(batch_dicts, run_logger=rl)
        except Exception as exc:  # noqa: BLE001
            _add_error(run, "web_search", f"batch {i}: {exc}")
            if rl:
                rl.error("web_search", f"Batch {i} failed: {exc}")
            log.error("web_search_batch_failed", index=i, error=str(exc))
            snippets_map = {}

        web_processed += len(batch)
        _set_stage(
            run,
            "web_search",
            status="running",
            processed=web_processed,
            total=total,
            pct=round(web_processed / max(total, 1) * 100, 1),
        )
        _set_stage(run, "llm_extraction", status="running", total=total)
        _commit(db)

        # LLM extraction — sequential within batch (low concurrency to stay under TPM)
        for prospect, prospect_dict in zip(batch, batch_dicts, strict=True):
            snippets = snippets_map.get(prospect.email)
            scraped = scrape_map.get(prospect.email)
            snippets_dict: dict[str, list]
            search_plan_dict: dict | None = None
            if snippets is None:
                snippets_dict = {"company_results": [], "person_results": []}
                if rl:
                    rl.log(
                        "llm_skipped",
                        stage="llm_extraction",
                        email=prospect.email,
                        reason="no_serper_snippets",
                    )
            else:
                snippets_dict = {
                    "company_results": snippets.company_results,
                    "person_results": snippets.person_results,
                }
                if snippets.plan is not None:
                    search_plan_dict = {
                        "company_query": snippets.plan.company_query,
                        "person_query": snippets.plan.person_query,
                        "serper_params": dict(snippets.plan.serper_params),
                    }

            profile_dict = {
                "country": prospect.country,
                "country_code": prospect.detected_country_code,
                "search_locale": prospect.search_locale,
                "language": prospect_dict.get("language"),
            }

            try:
                extracted = await llm_extractor.extract_fields(
                    prospect_dict,
                    snippets_dict,
                    taxonomy,
                    scraped=scraped,
                    run_logger=rl,
                )
            except Exception as exc:  # noqa: BLE001
                _add_error(run, "llm_extraction", f"{prospect.email}: {exc}")
                if rl:
                    rl.error("llm_extraction", f"{prospect.email}: {exc}", email=prospect.email)
                log.error("llm_extract_failed", email=prospect.email, error=str(exc))
                continue

            # Snapshot of the coerced LLM fields for the per-run trace + UI tabs
            llm_raw = {
                "industry": extracted.industry,
                "industry_confidence": extracted.industry_confidence,
                "sub_category": extracted.sub_category,
                "company_size": extracted.company_size,
                "relevance_score": extracted.relevance_score,
                "company_website": extracted.company_website,
                "company_linkedin": extracted.company_linkedin,
                "person_linkedin": extracted.person_linkedin,
                "company_description": extracted.company_description,
            }

            for k, v in llm_extractor.to_db_fields(
                extracted,
                snippets_dict,
                scraped=scraped,
                profile=profile_dict,
                search_plan=search_plan_dict,
                llm_raw=llm_raw,
            ).items():
                setattr(prospect, k, v)
            run.prospects_enriched = (run.prospects_enriched or 0) + 1
            llm_processed += 1

        _set_stage(
            run,
            "llm_extraction",
            status="running",
            processed=llm_processed,
            total=total,
            pct=round(llm_processed / max(total, 1) * 100, 1),
        )
        _commit(db)

    if rl:
        rl.stage_end("web_search", processed=web_processed, total=total)
        rl.stage_end("llm_extraction", processed=llm_processed, total=total)
    _set_stage(run, "web_search", status="completed", processed=web_processed, total=total)
    _set_stage(run, "llm_extraction", status="completed", processed=llm_processed, total=total)
    _commit(db)


async def run_pipeline(run_id: int) -> None:
    """Top-level entry point. Runs in a background task with its own DB session."""
    db = SessionLocal()
    rl: RunLogger | None = None
    try:
        run = db.query(PipelineRun).get(run_id)
        if run is None:
            log.error("pipeline_run_missing", run_id=run_id)
            return

        # Per-run JSONL trace logger — captures every external API call
        # (ZeroBounce, Serper, OpenAI) plus stage start/end events. Filename is
        # persisted on the PipelineRun row so the /pipeline/{id}/logs endpoint
        # can find the file later.
        rl = RunLogger(run_id=run_id)
        rl.log("pipeline_start", run_type=run.run_type, triggered_by=run.triggered_by)
        run.log_file = rl.filename

        run.status = "running"
        run.started_at = datetime.now(timezone.utc)
        run.progress = run.progress or _initial_progress()
        # Ingest already happened at upload time (per PRD §5 trigger model);
        # mark it complete from the orchestrator's perspective.
        _set_stage(
            run,
            "ingest",
            status="completed",
            processed=db.query(Prospect).count(),
        )
        _set_stage(run, "finalize", status="pending")
        _commit(db)

        run_type = run.run_type or "incremental"

        try:
            await _stage_email_validation(db, run, run_type, rl=rl)
            await _stage_data_profiling(db, run, run_type, rl=rl)
            # Pre-resolve the enrichment cohort so we only scrape sites for
            # prospects that the LLM stage will actually consume.
            enrichment_targets = select_prospects_to_enrich(db, run_type=run_type)
            scrape_map = await _stage_website_scraping(
                db, run, run_type, enrichment_targets, rl=rl
            )
            await _stage_enrichment(db, run, run_type, scrape_map=scrape_map, rl=rl)
        except Exception as exc:  # noqa: BLE001
            run.status = "failed"
            _add_error(run, "orchestrator", str(exc))
            if rl:
                rl.error("orchestrator", f"Pipeline failed: {exc}")
            log.error(
                "pipeline_failed",
                run_id=run_id,
                error=str(exc),
                trace=traceback.format_exc(),
            )
            run.completed_at = datetime.now(timezone.utc)
            _commit(db)
            if rl:
                rl.log("pipeline_end", status="failed", run_id=run_id)
            return

        _set_stage(run, "finalize", status="completed")
        run.status = "completed"
        run.completed_at = datetime.now(timezone.utc)
        run.total_records = db.query(Prospect).count()
        _commit(db)

        if rl:
            rl.log(
                "pipeline_end",
                status="completed",
                run_id=run_id,
                total_records=run.total_records,
                emails_validated=run.emails_validated,
                prospects_enriched=run.prospects_enriched,
                errors=run.errors,
            )
        log.info("pipeline_completed", run_id=run_id)
    finally:
        db.close()


def kickoff(db: Session, *, run_type: str, triggered_by: str | None = None) -> PipelineRun:
    """Create a new PipelineRun row and return it. Caller schedules the async runner."""
    if run_type not in {"full", "incremental"}:
        raise ValueError(f"Invalid run_type: {run_type!r}")
    if (
        db.query(PipelineRun)
        .filter(PipelineRun.status == "running")
        .first()
        is not None
    ):
        raise RuntimeError("A pipeline run is already in progress.")
    run = PipelineRun(
        run_type=run_type,
        status="pending",
        triggered_by=triggered_by,
        progress=_initial_progress(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def schedule(run_id: int) -> None:
    """Fire-and-forget the orchestrator on the running event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(run_pipeline(run_id))
        return
    loop.create_task(run_pipeline(run_id))


__all__ = ["kickoff", "run_pipeline", "schedule"]
