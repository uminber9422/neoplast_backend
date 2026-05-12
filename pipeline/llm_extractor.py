"""LLM-based extraction of structured fields from raw search snippets.

Uses OpenAI's chat completions API with a strict JSON schema. Falls back to
a deterministic mock when no API key is set so the pipeline can be exercised
end-to-end locally.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from backend.config import get_settings
from backend.logging_setup import get_logger
from backend.utils.columns import CANONICAL_FIELDS

log = get_logger(__name__)

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT = (
    "You are a B2B company research analyst. You receive multiple data "
    "sources about a company and a contact person, ranked by reliability:\n"
    "  1. The company's OWN website (scraped) — PRIMARY ground truth.\n"
    "  2. Structured prospect context from the user's CSV + a geo profile.\n"
    "  3. Web search snippets — supplementary, useful for filling gaps "
    "(LinkedIn URLs especially) but never overriding website content.\n"
    "Extract structured JSON only. Never invent data. If sources disagree, "
    "trust the website. If a field cannot be inferred from any source, use null."
)

COLUMN_DETECT_SYSTEM_PROMPT = (
    "You are a CSV schema detector. Given a list of column headers and a "
    "few sample rows from a B2B prospect spreadsheet, identify which header "
    "maps to each canonical field. Return JSON only. Never invent mappings — "
    "if a header doesn't clearly match any canonical field, omit it."
)


@dataclass
class ExtractedFields:
    company_website: str | None
    company_description: str | None
    industry: str
    industry_confidence: float
    sub_category: str | None
    company_size: str
    person_linkedin: str | None
    company_linkedin: str | None
    relevance_score: float


def _email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[-1].strip().lower() or None


def _scraped_section(scraped: dict | None) -> str:
    """Format the scraped-website portion of the prompt.

    Accepts a plain dict (from ``ScrapedSite.__dict__`` or equivalent) so this
    module doesn't have to import the website_scraper dataclass.
    """
    if not scraped or scraped.get("status") != "ok":
        # Surface the failure mode so the LLM knows the website wasn't reached
        if scraped:
            status = scraped.get("status") or "unknown"
            err = scraped.get("error") or ""
            return f"PRIMARY SOURCE — Company website: NOT AVAILABLE (status={status} {err})\n"
        return "PRIMARY SOURCE — Company website: NOT PROVIDED\n"

    parts: list[str] = [
        "PRIMARY SOURCE — Company website (scraped from "
        f"{scraped.get('final_url') or scraped.get('url')}):",
    ]
    if scraped.get("title"):
        parts.append(f"  Title: {scraped['title']}")
    if scraped.get("description"):
        parts.append(f"  Meta description: {scraped['description']}")
    text = (scraped.get("text") or "").strip()
    if text:
        parts.append(f"  Homepage text:\n{text}")
    about_text = (scraped.get("about_text") or "").strip()
    if about_text:
        about_url = scraped.get("about_url") or "(about page)"
        parts.append(f"  About page ({about_url}):\n{about_text}")
    socials = scraped.get("social_links") or {}
    if socials:
        parts.append(f"  Social links found on site: {json.dumps(socials, ensure_ascii=False)}")
    return "\n".join(parts) + "\n"


def _prospect_context_section(prospect: dict) -> str:
    """Format the structured prospect / profile context."""
    name = prospect.get("name") or "Unknown"
    email = prospect.get("email")
    domain = _email_domain(email)
    company = prospect.get("company_name") or "Unknown"
    city = prospect.get("city") or "?"
    state = prospect.get("state") or "?"
    country = prospect.get("country") or "?"
    country_code = prospect.get("detected_country_code") or prospect.get("country_code")
    language = prospect.get("language")
    notes = (prospect.get("notes") or "").strip()
    website_csv = prospect.get("website_csv")

    lines = [
        "PROSPECT CONTEXT (from CSV + Phase 2 data profiler):",
        f"  Contact: {name}",
        f"  Email: {email or 'Unknown'}" + (f" (domain: {domain})" if domain else ""),
        f"  Company: {company}",
        f"  Location: {city}, {state}, {country}"
        + (f" [{country_code}]" if country_code else ""),
    ]
    if language:
        lines.append(f"  Likely working language: {language}")
    if website_csv:
        lines.append(f"  CSV-supplied website: {website_csv}")
    if notes:
        lines.append(f"  CSV notes: {notes}")
    return "\n".join(lines) + "\n"


def _snippets_section(snippets: dict | None) -> str:
    snippets = snippets or {}
    company_results = snippets.get("company_results") or []
    person_results = snippets.get("person_results") or []
    if not company_results and not person_results:
        return "SUPPLEMENTARY — Web search snippets: (none — website data is the only source)\n"
    out = ["SUPPLEMENTARY — Web search snippets (use to fill gaps, NOT to override website):"]
    out.append(
        "  Company results:\n"
        + json.dumps(company_results, ensure_ascii=False, indent=2)
    )
    out.append(
        "  Person results:\n"
        + json.dumps(person_results, ensure_ascii=False, indent=2)
    )
    return "\n".join(out) + "\n"


def _build_user_prompt(
    *,
    prospect: dict,
    scraped: dict | None,
    snippets: dict | None,
    taxonomy: list[str],
) -> str:
    """Compose the multi-source prompt: profile + scraped site + snippets."""
    return (
        _prospect_context_section(prospect)
        + "\n"
        + _scraped_section(scraped)
        + "\n"
        + _snippets_section(snippets)
        + "\n"
        + "Return JSON ONLY, matching this exact shape:\n"
        "{\n"
        '  "company_website": string|null (use the scraped final_url when available),\n'
        '  "company_description": string|null (1-2 sentences; prefer the company\'s own description),\n'
        f'  "industry": one of {taxonomy},\n'
        '  "industry_confidence": float 0.0-1.0 (raise it when scraped text is the basis),\n'
        '  "sub_category": string|null,\n'
        '  "company_size": "micro"|"small"|"medium"|"large"|"unknown",\n'
        '  "person_linkedin": string|null (LinkedIn /in/... URL),\n'
        '  "company_linkedin": string|null (LinkedIn /company/... URL — '
        "prefer scraped social link, fall back to search results),\n"
        '  "relevance_to_plastics_machinery": float 0.0-1.0\n'
        "}"
    )


def _mock_extract(
    *,
    company_name: str | None,
    snippets: dict | None,
    scraped: dict | None,
    taxonomy: list[str],
) -> ExtractedFields:
    """Deterministic-ish mock that honors all three signal sources.

    Priority: scraped website > Serper snippets > falls back to ``None``.
    Mirrors the production prompt's hierarchy so mock-mode pipelines exercise
    the same data flow as a real LLM call would.
    """
    snippets = snippets or {}
    rng = random.Random(
        hash((company_name or "x") + json.dumps(snippets) + json.dumps(scraped or {}))
    )
    industry = rng.choice([i for i in taxonomy if i != "Unknown"])

    # Website (scraped final_url wins; else first non-LinkedIn search result)
    website: str | None = None
    if scraped and scraped.get("status") == "ok":
        website = scraped.get("final_url") or scraped.get("url")
    if not website:
        website = next(
            (
                r.get("link")
                for r in snippets.get("company_results", [])
                if "linkedin" not in (r.get("link") or "")
            ),
            None,
        )

    # Description: scraped meta/text wins; else first search snippet
    description: str | None = None
    if scraped and scraped.get("status") == "ok":
        description = scraped.get("description") or (scraped.get("text") or "")[:280] or None
    if not description and snippets.get("company_results"):
        description = snippets["company_results"][0].get("snippet")

    # Company LinkedIn: scraped social link wins
    company_link: str | None = None
    if scraped and scraped.get("status") == "ok":
        socials = scraped.get("social_links") or {}
        company_link = socials.get("linkedin")
    if not company_link:
        company_link = next(
            (
                r.get("link")
                for r in snippets.get("company_results", [])
                if "linkedin.com/company" in (r.get("link") or "")
            ),
            None,
        )

    # Person LinkedIn — only Serper has this
    person_link = next(
        (
            r.get("link")
            for r in snippets.get("person_results", [])
            if "linkedin.com/in" in (r.get("link") or "")
        ),
        None,
    )

    # Confidence boost when the scrape succeeded (we trust it more)
    base_low, base_high = (0.7, 0.95) if (scraped and scraped.get("status") == "ok") else (0.55, 0.85)

    return ExtractedFields(
        company_website=website,
        company_description=description,
        industry=industry,
        industry_confidence=round(rng.uniform(base_low, base_high), 2),
        sub_category=None,
        company_size=rng.choice(["micro", "small", "medium", "large", "unknown"]),
        person_linkedin=person_link,
        company_linkedin=company_link,
        relevance_score=round(rng.uniform(0.3, 0.95), 2),
    )


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
async def _call_openai(
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    user_prompt: str,
) -> dict:
    """Call OpenAI chat completions, return ``{parsed, usage, model}``.

    The richer return shape (usage / model rather than just the parsed JSON)
    feeds the per-run JSONL trace logger so the operator can see token costs
    and which model actually served the response.
    """
    response = await client.post(
        OPENAI_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        },
        timeout=30.0,
    )
    response.raise_for_status()
    body = response.json()
    content = body["choices"][0]["message"]["content"]
    return {
        "parsed": json.loads(content),
        "usage": body.get("usage"),
        "model": body.get("model"),
    }


def _coerce(data: dict, taxonomy: list[str]) -> ExtractedFields:
    """Defensive coercion: model output isn't always 100% schema-conformant."""
    industry = data.get("industry") or "Unknown"
    if industry not in taxonomy:
        industry = "Unknown"
    company_size = data.get("company_size") or "unknown"
    if company_size not in {"micro", "small", "medium", "large", "unknown"}:
        company_size = "unknown"

    def _bounded_float(v) -> float:
        try:
            f = float(v)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, f))

    return ExtractedFields(
        company_website=data.get("company_website") or None,
        company_description=data.get("company_description") or None,
        industry=industry,
        industry_confidence=_bounded_float(data.get("industry_confidence")),
        sub_category=data.get("sub_category") or None,
        company_size=company_size,
        person_linkedin=data.get("person_linkedin") or None,
        company_linkedin=data.get("company_linkedin") or None,
        relevance_score=_bounded_float(
            data.get("relevance_to_plastics_machinery") or data.get("relevance_score")
        ),
    )


async def extract_fields(
    prospect: dict,
    snippets: dict | None = None,
    taxonomy: list[str] | None = None,
    *,
    scraped: dict | None = None,
    run_logger=None,
) -> ExtractedFields:
    """Extract structured fields for a single prospect.

    Sources, in priority order:
        1. ``scraped`` — output of the website scraper (Phase 4) as a dict
           (typically ``ScrapedSite.__dict__`` or a JSON-friendly version).
        2. ``prospect`` — the CSV record plus the data-profiler fields
           (``country``, ``language``, etc.) added in Phase 2.
        3. ``snippets`` — Serper results (Phase 5). Used to fill gaps.

    All sources are optional; pass what you have. The mock-mode path mirrors
    this same priority so end-to-end testing without an API key produces
    realistic-looking results.

    When ``run_logger`` is provided, every request / response / coerce step
    is appended as a JSONL event for the per-run trace log.
    """
    if taxonomy is None:
        taxonomy = ["Unknown"]
    settings = get_settings()
    api_key = settings.OPENAI_API_KEY
    email = prospect.get("email", "?")

    if not api_key:
        if run_logger:
            run_logger.log(
                "llm_mock_mode",
                stage="llm_extraction",
                email=email,
                message="No API key — using mock extraction",
            )
        return _mock_extract(
            company_name=prospect.get("company_name"),
            snippets=snippets,
            scraped=scraped,
            taxonomy=taxonomy,
        )

    user_prompt = _build_user_prompt(
        prospect=prospect,
        scraped=scraped,
        snippets=snippets,
        taxonomy=taxonomy,
    )

    if run_logger:
        run_logger.log(
            "llm_request",
            stage="llm_extraction",
            email=email,
            model=settings.LLM_MODEL,
            request={
                "system_prompt": SYSTEM_PROMPT,
                "user_prompt": user_prompt,
                "temperature": 0.1,
            },
        )

    async with httpx.AsyncClient() as client:
        try:
            result = await _call_openai(client, api_key, settings.LLM_MODEL, user_prompt)
            data = result["parsed"]
            if run_logger:
                run_logger.log(
                    "llm_response",
                    stage="llm_extraction",
                    email=email,
                    model=result.get("model"),
                    response=data,
                    usage=result.get("usage"),
                )
        except Exception as exc:  # noqa: BLE001
            log.error("llm_call_failed", email=email, error=str(exc))
            if run_logger:
                run_logger.error(
                    "llm_extraction",
                    f"LLM call failed for {email}: {exc}",
                    email=email,
                )
            return _mock_extract(
                company_name=prospect.get("company_name"),
                snippets=snippets,
                scraped=scraped,
                taxonomy=taxonomy,
            )

    coerced = _coerce(data, taxonomy)

    if run_logger:
        run_logger.log(
            "llm_coerced",
            stage="llm_extraction",
            email=email,
            coerced_fields={
                "industry": coerced.industry,
                "industry_confidence": coerced.industry_confidence,
                "sub_category": coerced.sub_category,
                "company_size": coerced.company_size,
                "relevance_score": coerced.relevance_score,
                "company_website": coerced.company_website,
                "company_linkedin": coerced.company_linkedin,
                "person_linkedin": coerced.person_linkedin,
            },
        )

    return coerced


def to_db_fields(
    extracted: ExtractedFields,
    snippets: dict | None = None,
    *,
    scraped: dict | None = None,
    profile: dict | None = None,
    search_plan: dict | None = None,
    llm_raw: dict | None = None,
) -> dict:
    """Adapter from ExtractedFields to Prospect ORM column values.

    ``enrichment_raw`` now stores the **full data chain** so the pipeline-logs
    UI and any future debugging session can see exactly which sources were
    available when the LLM made its call:

        - ``snippets``         : Serper company / person search results
        - ``scraped``          : ScrapedSite dict (Phase 4)
        - ``profile``          : Geo profile dict (Phase 2)
        - ``search_plan``      : The plan that produced the snippets (Phase 5)
        - ``llm_response``     : Raw LLM JSON output (when provided)
        - ``extracted_fields`` : Coerced fields shown in the Prospect detail UI
    """
    enrichment_raw: dict = {"snippets": snippets or {}}
    if scraped is not None:
        enrichment_raw["scraped"] = scraped
    if profile is not None:
        enrichment_raw["profile"] = profile
    if search_plan is not None:
        enrichment_raw["search_plan"] = search_plan
    if llm_raw is not None:
        enrichment_raw["llm_response"] = llm_raw
    # Compact summary of what the prospect actually got — useful for the
    # "Enrichment Data" detail tab that reads a flat key-value list.
    enrichment_raw["extracted_fields"] = {
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

    return {
        "company_website": extracted.company_website,
        "company_linkedin": extracted.company_linkedin,
        "person_linkedin": extracted.person_linkedin,
        "company_description": extracted.company_description,
        "industry": extracted.industry,
        "industry_confidence": extracted.industry_confidence,
        "sub_category": extracted.sub_category,
        "company_size": extracted.company_size,
        "relevance_score": extracted.relevance_score,
        "enrichment_raw": enrichment_raw,
        "enriched_at": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Column detection (CSV schema fallback)
# ---------------------------------------------------------------------------
def _build_column_detect_prompt(
    headers: list[str],
    sample_rows: list[dict],
) -> str:
    canonical = ", ".join(CANONICAL_FIELDS)
    return (
        "Headers in the CSV file:\n"
        f"{json.dumps(headers, ensure_ascii=False)}\n\n"
        "First few sample rows (use these to disambiguate column meaning):\n"
        f"{json.dumps(sample_rows[:5], ensure_ascii=False, indent=2)}\n\n"
        "Map each header that you can identify to ONE canonical field from "
        "this list:\n"
        f"  {canonical}\n\n"
        "Notes:\n"
        "- Use 'name' for any contact-person column (full name, contact "
        "person, decision maker, owner, etc).\n"
        "- Use 'company_name' for the business / organization column.\n"
        "- Use 'website_csv' for any company website / URL column (this is "
        "distinct from internal LinkedIn fields).\n"
        "- Use 'country' for nation / country / country-code columns.\n"
        "- Skip ID columns, dates, internal codes, and anything ambiguous.\n"
        "- Each canonical field may receive at most one header.\n\n"
        "Return JSON object with header strings as keys and canonical field "
        'names as values, e.g. {"NATION": "country", "EMAIL": "email"}.'
    )


def _coerce_column_mapping(
    raw: dict,
    headers: list[str],
) -> dict[str, str]:
    """Strip anything from the LLM response that isn't a real header → canonical pair."""
    if not isinstance(raw, dict):
        return {}
    headers_set = set(headers)
    valid_canonical = set(CANONICAL_FIELDS)
    mapping: dict[str, str] = {}
    for header, canonical in raw.items():
        if not isinstance(header, str) or not isinstance(canonical, str):
            continue
        if header not in headers_set:
            continue
        if canonical not in valid_canonical:
            continue
        # First write wins — guards against the LLM emitting duplicates.
        if header not in mapping:
            mapping[header] = canonical
    return mapping


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(2),  # one retry — column detection is best-effort
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
def _call_openai_sync(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
) -> dict:
    """Sync sibling of :func:`_call_openai`, used from the synchronous ingest path."""
    response = httpx.post(
        OPENAI_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        },
        timeout=20.0,
    )
    response.raise_for_status()
    body = response.json()
    content = body["choices"][0]["message"]["content"]
    return json.loads(content)


def detect_columns(
    headers: list[str],
    sample_rows: list[dict],
) -> dict[str, str]:
    """LLM-powered fallback for CSV column → canonical-field mapping.

    Returns ``{header: canonical_field}`` for the headers the model was
    confident about. Returns an empty dict when no API key is configured
    (callers should treat that as "no help available" and fall back to
    whatever fuzzy mapping they already had).

    Synchronous on purpose — this is a one-shot call invoked once per file
    upload from :func:`backend.pipeline.ingest.ingest_file`, which is itself
    synchronous. No need to spin up an event loop.
    """
    settings = get_settings()
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        log.info("detect_columns_mock_mode", header_count=len(headers))
        return {}

    if not headers:
        return {}

    user_prompt = _build_column_detect_prompt(headers, sample_rows)
    try:
        raw = _call_openai_sync(
            api_key,
            settings.LLM_MODEL,
            COLUMN_DETECT_SYSTEM_PROMPT,
            user_prompt,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("detect_columns_failed", error=str(exc), header_count=len(headers))
        return {}

    mapping = _coerce_column_mapping(raw, headers)
    log.info(
        "detect_columns_complete",
        header_count=len(headers),
        mapped=len(mapping),
        canonical_fields=sorted(set(mapping.values())),
    )
    return mapping


__all__ = [
    "ExtractedFields",
    "detect_columns",
    "extract_fields",
    "to_db_fields",
]
