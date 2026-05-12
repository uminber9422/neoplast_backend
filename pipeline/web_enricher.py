"""Web search enrichment via Serper API (or mock when key absent).

Query design (Phase 5 rewrite):
    - **No hardcoded "India" or "manufacturer"** — queries adapt to the
      prospect's profile (Phase 2: country, search_locale, language).
    - Company name is exact-quoted so Serper doesn't substring-match it.
    - Geo params (``gl``, ``hl``) come from the prospect's locale/language
      rather than a global default.
    - Person search uses ``site:linkedin.com`` to focus on profile pages.
    - When the website scraper (Phase 4) already produced rich company data
      we **skip the company Serper query** entirely and let Serper's budget
      go to person discovery — strictly higher quality than scraping LinkedIn
      from a search snippet.

Pure construction logic lives in :func:`build_search_plan` so it can be
unit-tested without any HTTP I/O.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from backend.config import get_settings
from backend.logging_setup import get_logger

log = get_logger(__name__)

SERPER_URL = "https://google.serper.dev/search"


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------
@dataclass
class SearchPlan:
    """Pure plan for what to ask Serper for a single prospect.

    A ``None`` query means "skip this lookup entirely." The plan is built
    deterministically from the prospect dict — useful for tests and for the
    pipeline-logs UI to show *why* a particular query was issued.
    """

    company_query: str | None
    person_query: str | None
    serper_params: dict[str, str] = field(default_factory=dict)


@dataclass
class SearchSnippets:
    """Collected raw search context for one prospect, plus the plan that
    produced it (for observability)."""

    company_results: list[dict]
    person_results: list[dict]
    plan: SearchPlan | None = None


# ---------------------------------------------------------------------------
# Pure query construction (no I/O — easy to test)
# ---------------------------------------------------------------------------
def _quote_for_search(value: str) -> str:
    """Wrap a value in double-quotes for exact-match. Strips inner quotes
    (Serper treats them as separators)."""
    cleaned = value.replace('"', "").strip()
    return f'"{cleaned}"' if cleaned else ""


def build_search_plan(prospect: dict) -> SearchPlan:
    """Decide what queries (if any) to run for a single prospect.

    The prospect dict is the same shape the orchestrator already builds (see
    ``orchestrator.py``), extended with optional fields that Phase 2 / Phase 4
    populate:

        - ``country``         — full country name (e.g. "Algeria")
        - ``search_locale``   — Serper ``gl`` parameter (e.g. ``"dz"``)
        - ``language``        — Serper ``hl`` parameter (e.g. ``"fr"``)
        - ``has_scraped_data`` — bool; when True we skip the company query

    All optional. Missing fields just produce a less-targeted query (still
    correct, just lower-recall).
    """
    name = (prospect.get("name") or "").strip()
    company = (prospect.get("company_name") or "").strip()
    country = (prospect.get("country") or "").strip()
    city = (prospect.get("city") or "").strip()
    locale = (prospect.get("search_locale") or "").strip().lower()
    language = (prospect.get("language") or "").strip().lower()
    has_scraped = bool(prospect.get("has_scraped_data"))

    # ---- Company query ---------------------------------------------------
    company_query: str | None
    if has_scraped:
        # The website scraper already pulled the company description and
        # social links — no point burning Serper budget here too.
        company_query = None
    else:
        parts: list[str] = []
        if company:
            parts.append(_quote_for_search(company))
        # Geo grounding: prefer country (most stable), fall back to city.
        if country:
            parts.append(country)
        elif city:
            parts.append(city)
        company_query = " ".join(p for p in parts if p) or None

    # ---- Person query ----------------------------------------------------
    # site:linkedin.com narrows Serper to LinkedIn profile pages, which is
    # where the high-signal person data lives. Without a name we can't
    # reasonably search for a person.
    person_query: str | None = None
    if name:
        person_parts: list[str] = ["site:linkedin.com", _quote_for_search(name)]
        if company:
            person_parts.append(_quote_for_search(company))
        person_query = " ".join(p for p in person_parts if p)

    # ---- Geo params for Serper ------------------------------------------
    serper_params: dict[str, str] = {}
    if locale:
        serper_params["gl"] = locale
    if language:
        serper_params["hl"] = language

    return SearchPlan(
        company_query=company_query,
        person_query=person_query,
        serper_params=serper_params,
    )


# ---------------------------------------------------------------------------
# Mock results (no API key)
# ---------------------------------------------------------------------------
def _mock_results(prospect: dict, plan: SearchPlan) -> SearchSnippets:
    """Deterministic mock so the pipeline can run end-to-end locally.

    Honors the plan: if the company query was skipped we return an empty
    company_results (matching what the live path would do).
    """
    name = prospect.get("name")
    company = prospect.get("company_name")
    country = prospect.get("country")
    city = prospect.get("city") or country or "their city"

    company_label = company or "Unknown Co"
    company_results: list[dict] = []
    if plan.company_query:
        company_results = [
            {
                "title": f"{company_label} | About Us",
                "link": f"https://www.{(company or 'example').lower().replace(' ', '')}.example",
                "snippet": (
                    f"{company_label} is a B2B supplier based in "
                    f"{country or city}, serving industrial customers."
                ),
            },
            {
                "title": f"{company_label} on LinkedIn",
                "link": f"https://www.linkedin.com/company/{(company or 'example').lower().replace(' ', '-')}",
                "snippet": f"View {company_label} on LinkedIn.",
            },
        ]

    person_results: list[dict] = []
    if plan.person_query and name:
        person_results = [
            {
                "title": f"{name} - {company_label}",
                "link": f"https://www.linkedin.com/in/{name.lower().replace(' ', '-')}",
                "snippet": f"{name} works at {company_label}.",
            }
        ]

    return SearchSnippets(
        company_results=company_results,
        person_results=person_results,
        plan=plan,
    )


# ---------------------------------------------------------------------------
# Live Serper call
# ---------------------------------------------------------------------------
@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
async def _serper_query(
    client: httpx.AsyncClient,
    api_key: str,
    query: str,
    params: dict[str, str],
    *,
    email: str | None = None,
    query_type: str = "company",
    run_logger=None,
) -> list[dict]:
    payload: dict[str, object] = {"q": query, "num": 5}
    payload.update(params)

    if run_logger:
        run_logger.log(
            "serper_request",
            stage="web_search",
            email=email,
            query_type=query_type,
            request={"url": SERPER_URL, "query": query, "params": payload},
        )

    response = await client.post(
        SERPER_URL,
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=15.0,
    )
    response.raise_for_status()
    data = response.json()
    organic = data.get("organic", [])[:5]

    if run_logger:
        run_logger.log(
            "serper_response",
            stage="web_search",
            email=email,
            query_type=query_type,
            results_count=len(organic),
            response={
                "organic": organic,
                "search_information": data.get("searchInformation"),
            },
            http_status=response.status_code,
        )

    return organic


# ---------------------------------------------------------------------------
# Per-prospect orchestration
# ---------------------------------------------------------------------------
async def enrich_prospect(
    client: httpx.AsyncClient | None,
    api_key: str,
    prospect: dict,
    *,
    run_logger=None,
) -> SearchSnippets:
    """Run the (up to) two Serper queries for one prospect.

    The ``prospect`` dict carries everything we need; profile/scrape status
    are optional fields on it. Returns empty result lists when both queries
    were skipped (still a valid SearchSnippets).
    """
    plan = build_search_plan(prospect)
    email = prospect.get("email")

    if not api_key:
        if run_logger:
            run_logger.log(
                "serper_mock_mode",
                stage="web_search",
                email=email,
                message="No API key — using mock results",
            )
        return _mock_results(prospect, plan)

    assert client is not None  # noqa: S101 — guarded by api_key check above

    async def _run(query: str | None, query_type: str) -> list[dict]:
        if not query:
            return []
        return await _serper_query(
            client,
            api_key,
            query,
            plan.serper_params,
            email=email,
            query_type=query_type,
            run_logger=run_logger,
        )

    try:
        company_results, person_results = await asyncio.gather(
            _run(plan.company_query, "company"),
            _run(plan.person_query, "person"),
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "serper_query_failed",
            email=email,
            error=str(exc),
            company_query=plan.company_query,
            person_query=plan.person_query,
        )
        if run_logger:
            run_logger.error(
                "web_search",
                f"Serper failed for {email}: {exc}",
                email=email,
            )
        return SearchSnippets(company_results=[], person_results=[], plan=plan)

    return SearchSnippets(
        company_results=company_results,
        person_results=person_results,
        plan=plan,
    )


async def enrich_batch(
    prospects: list[dict],
    *,
    concurrency: int = 5,
    run_logger=None,
) -> dict[str, SearchSnippets]:
    """Run web enrichment for a batch. Returns ``{email: SearchSnippets}``."""
    settings = get_settings()
    api_key = settings.SERPER_API_KEY
    if not api_key:
        log.warning("web_enrichment_mock_mode", count=len(prospects))

    results: dict[str, SearchSnippets] = {}
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:

        async def _one(p: dict) -> None:
            async with semaphore:
                results[p["email"]] = await enrich_prospect(
                    client if api_key else None,
                    api_key,
                    p,
                    run_logger=run_logger,
                )

        await asyncio.gather(*(_one(p) for p in prospects))

    return results


__all__ = [
    "SearchPlan",
    "SearchSnippets",
    "build_search_plan",
    "enrich_batch",
    "enrich_prospect",
]
