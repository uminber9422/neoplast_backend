"""Website scraper — direct extraction of prospect data from their own site.

For prospects with a CSV-supplied website we'd rather scrape the page than
rely on whatever a search engine snippet happens to surface. The site itself
has the company's own description, social links, and (often) an /about page —
all primary sources that the LLM extractor (Phase 6) treats as ground truth.

Design constraints:
    - Pure stdlib HTML parsing (no bs4 dep added) — html.parser is sufficient
      for the structured fields we want.
    - Async via httpx, batched with bounded concurrency.
    - Honors robots.txt with a permissive default (network errors → allow).
    - Per-page byte cap and request timeout to avoid runaway scrapes.
    - Returns a structured :class:`ScrapedSite` even on failure (the pipeline
      should never crash because one site went down).

Phase 7 wires this into the orchestrator. Phase 4 ships only the standalone
module + config knobs.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx

from backend.config import get_settings
from backend.logging_setup import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------
@dataclass
class ScrapedSite:
    """Structured result of a website scrape.

    A ``ScrapedSite`` is always returned (never ``None``); ``status`` carries
    the outcome so the caller can decide how to react. ``error`` is populated
    only when the scrape didn't complete normally.
    """

    url: str                              # the input URL after normalization
    status: str                           # see _STATUS_* constants below
    final_url: str | None = None          # after redirects
    title: str | None = None
    description: str | None = None        # meta description / og:description
    text: str = ""                        # cleaned visible text from homepage
    about_url: str | None = None
    about_text: str = ""
    social_links: dict[str, str] = field(default_factory=dict)
    status_code: int | None = None
    error: str | None = None
    scraped_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Status outcomes:
_STATUS_OK = "ok"
_STATUS_SKIPPED = "skipped"               # disabled or empty URL
_STATUS_BLOCKED = "blocked_by_robots"
_STATUS_HTTP_ERROR = "http_error"
_STATUS_TIMEOUT = "timeout"
_STATUS_NOT_HTML = "not_html"
_STATUS_NETWORK_ERROR = "network_error"


# ---------------------------------------------------------------------------
# HTML parsing — stdlib html.parser
# ---------------------------------------------------------------------------
_SKIP_TEXT_TAGS: frozenset[str] = frozenset(
    {"script", "style", "noscript", "svg", "head"}
)


class _SiteParser(HTMLParser):
    """Single-pass extractor for title, meta description, links, and text.

    Tracks a depth counter for tags whose content should not contribute to
    the visible-text pool (script/style/etc.). Collects ``<a href>`` with
    its anchor text so we can later score links for "looks like an about
    page" or "looks like LinkedIn."
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title: str | None = None
        self.meta_description: str | None = None
        self.og_description: str | None = None
        self.links: list[tuple[str, str]] = []   # (href, anchor_text)
        self._text_chunks: list[str] = []
        self._in_title: bool = False
        # Depth counter — head is opened/closed once but we still want to
        # skip its contents (except the title/meta tags we capture).
        self._skip_depth: int = 0
        self._current_link_href: str | None = None
        self._current_link_text: list[str] = []

    # -- starttag / endtag --------------------------------------------------
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_d = {k.lower(): (v or "") for k, v in attrs}
        if tag == "title":
            self._in_title = True
            return
        if tag == "meta":
            name = attrs_d.get("name", "").lower()
            prop = attrs_d.get("property", "").lower()
            content = attrs_d.get("content", "").strip()
            if not content:
                return
            if name == "description" and not self.meta_description:
                self.meta_description = content
            elif prop == "og:description" and not self.og_description:
                self.og_description = content
            return
        if tag == "a":
            href = attrs_d.get("href", "").strip()
            if href and not href.startswith("#") and not href.startswith("javascript:"):
                self._current_link_href = href
                self._current_link_text = []
            return
        if tag in _SKIP_TEXT_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
            return
        if tag == "a" and self._current_link_href is not None:
            text = " ".join(self._current_link_text).strip()
            self.links.append((self._current_link_href, text))
            self._current_link_href = None
            self._current_link_text = []
            return
        if tag in _SKIP_TEXT_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1

    # -- character data -----------------------------------------------------
    def handle_data(self, data: str) -> None:
        if self._in_title:
            cleaned = data.strip()
            if cleaned:
                self.title = (self.title or "") + cleaned
            return
        if self._skip_depth > 0:
            return
        cleaned = data.strip()
        if not cleaned:
            return
        if self._current_link_href is not None:
            self._current_link_text.append(cleaned)
        self._text_chunks.append(cleaned)

    # -- consolidated text --------------------------------------------------
    def visible_text(self, max_chars: int) -> str:
        joined = " ".join(self._text_chunks)
        # Collapse runs of whitespace
        collapsed = re.sub(r"\s+", " ", joined).strip()
        if len(collapsed) > max_chars:
            collapsed = collapsed[:max_chars]
        return collapsed


# ---------------------------------------------------------------------------
# URL normalization & link analysis
# ---------------------------------------------------------------------------
_SOCIAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("linkedin", re.compile(r"^https?://(?:[a-z]{2,3}\.)?linkedin\.com/(?:company|in|school)/", re.I)),
    ("twitter", re.compile(r"^https?://(?:www\.)?(?:twitter|x)\.com/[^/]+/?$", re.I)),
    ("facebook", re.compile(r"^https?://(?:www\.|m\.)?(?:facebook|fb)\.com/[^/]+/?", re.I)),
    ("instagram", re.compile(r"^https?://(?:www\.)?instagram\.com/[^/]+/?", re.I)),
    ("youtube", re.compile(r"^https?://(?:www\.)?youtube\.com/(?:c/|channel/|@|user/)", re.I)),
)

_ABOUT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"/about(?:[/?#-]|$|-?us\b)", re.I),
    re.compile(r"/company(?:[/?#-]|$)", re.I),
    re.compile(r"/who-?we-?are(?:[/?#-]|$)", re.I),
    re.compile(r"/qui[-_]sommes[-_]nous", re.I),
    re.compile(r"/a[-_]propos", re.I),
    re.compile(r"/uber[-_]uns", re.I),
    re.compile(r"/sobre[-_]nos", re.I),
    re.compile(r"/nosotros", re.I),
)


def _normalize_url(raw: str | None) -> str | None:
    """Add a scheme if missing, strip trailing slash, and validate parseability."""
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    if not text.lower().startswith(("http://", "https://")):
        text = "https://" + text
    parsed = urlparse(text)
    if not parsed.netloc:
        return None
    # Rebuild without trailing slash on the path
    path = parsed.path.rstrip("/") if parsed.path != "/" else ""
    rebuilt = f"{parsed.scheme}://{parsed.netloc}{path}"
    if parsed.query:
        rebuilt += f"?{parsed.query}"
    return rebuilt


def _extract_socials(links: list[tuple[str, str]], base_url: str) -> dict[str, str]:
    """Pull social-network URLs out of the link list, one per platform."""
    found: dict[str, str] = {}
    for href, _text in links:
        absolute = urljoin(base_url, href)
        for platform, pattern in _SOCIAL_PATTERNS:
            if platform in found:
                continue
            if pattern.match(absolute):
                found[platform] = absolute
                break
    return found


def _find_about_link(links: list[tuple[str, str]], base_url: str) -> str | None:
    """Find the first link that looks like an about / company page."""
    base_host = urlparse(base_url).netloc.lower()
    for href, text in links:
        absolute = urljoin(base_url, href)
        # Only follow same-host links
        if urlparse(absolute).netloc.lower() != base_host:
            continue
        path = urlparse(absolute).path or "/"
        for pattern in _ABOUT_PATTERNS:
            if pattern.search(path):
                return absolute
        # Anchor-text fallback ("About us", "Notre entreprise", etc.)
        text_lower = text.lower()
        if text_lower in {"about", "about us", "company", "qui sommes-nous", "à propos"}:
            return absolute
    return None


def _description_from(parser: _SiteParser) -> str | None:
    """Pick the best description from the parsed page."""
    if parser.meta_description:
        return parser.meta_description.strip()
    if parser.og_description:
        return parser.og_description.strip()
    return None


# ---------------------------------------------------------------------------
# Robots.txt — best-effort, default-allow on errors.
# ---------------------------------------------------------------------------
async def _is_allowed_by_robots(
    client: httpx.AsyncClient,
    target_url: str,
    user_agent: str,
    timeout: float,
) -> bool:
    parsed = urlparse(target_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        resp = await client.get(robots_url, timeout=timeout)
    except (httpx.HTTPError, httpx.TimeoutException):
        return True
    if resp.status_code != 200:
        return True
    try:
        rp = RobotFileParser()
        rp.parse(resp.text.splitlines())
        return rp.can_fetch(user_agent, target_url)
    except Exception:  # noqa: BLE001
        return True


# ---------------------------------------------------------------------------
# Single-page fetch
# ---------------------------------------------------------------------------
async def _fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    user_agent: str,
    timeout: float,
    max_bytes: int,
) -> tuple[str | None, str | None, int | None, str | None]:
    """Fetch a URL, return (html_text, final_url, status_code, error_status).

    ``error_status`` is one of the _STATUS_* constants when the fetch failed,
    or None on success. Caller maps that into the parent ScrapedSite.
    """
    try:
        resp = await client.get(
            url,
            headers={"User-Agent": user_agent, "Accept": "text/html,*/*;q=0.5"},
            timeout=timeout,
            follow_redirects=True,
        )
    except httpx.TimeoutException as exc:
        return None, None, None, _STATUS_TIMEOUT
    except httpx.HTTPError as exc:
        log.debug("scrape_network_error", url=url, error=str(exc))
        return None, None, None, _STATUS_NETWORK_ERROR

    final_url = str(resp.url)
    if resp.status_code >= 400:
        return None, final_url, resp.status_code, _STATUS_HTTP_ERROR

    content_type = resp.headers.get("content-type", "").lower()
    if content_type and "html" not in content_type and "xml" not in content_type:
        return None, final_url, resp.status_code, _STATUS_NOT_HTML

    body = resp.content[:max_bytes]
    # httpx will pick an encoding from headers; fall back to utf-8 with
    # replacement so a single weird byte doesn't blow up the whole parse.
    try:
        text = body.decode(resp.encoding or "utf-8", errors="replace")
    except (LookupError, TypeError):
        text = body.decode("utf-8", errors="replace")
    return text, final_url, resp.status_code, None


# ---------------------------------------------------------------------------
# Public scrape API
# ---------------------------------------------------------------------------
async def scrape_site(
    url: str | None,
    *,
    client: httpx.AsyncClient | None = None,
    settings: object | None = None,
) -> ScrapedSite:
    """Scrape a single website. Always returns a :class:`ScrapedSite`.

    Pass an existing ``client`` to reuse a connection pool across many sites
    (see :func:`scrape_batch`); when ``client`` is ``None`` we open and close
    one for this single call.
    """
    cfg = settings or get_settings()
    normalized = _normalize_url(url)
    if not normalized or not getattr(cfg, "WEBSITE_SCRAPE_ENABLED", True):
        return ScrapedSite(
            url=normalized or (url or ""),
            status=_STATUS_SKIPPED,
            error="website_scrape_disabled" if not getattr(cfg, "WEBSITE_SCRAPE_ENABLED", True) else "empty_or_invalid_url",
        )

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient()
    try:
        return await _scrape_with_client(client, normalized, cfg)
    finally:
        if own_client:
            await client.aclose()


async def _scrape_with_client(
    client: httpx.AsyncClient,
    url: str,
    cfg: object,
) -> ScrapedSite:
    user_agent = getattr(cfg, "WEBSITE_SCRAPE_USER_AGENT", "NeoplastBot/1.0")
    timeout = float(getattr(cfg, "WEBSITE_SCRAPE_TIMEOUT_SECONDS", 10.0))
    max_pages = int(getattr(cfg, "WEBSITE_SCRAPE_MAX_PAGES", 2))
    max_bytes = int(getattr(cfg, "WEBSITE_SCRAPE_MAX_BYTES", 1_000_000))
    max_text = int(getattr(cfg, "WEBSITE_SCRAPE_MAX_TEXT_CHARS", 50_000))

    # Robots check on the homepage
    allowed = await _is_allowed_by_robots(client, url, user_agent, timeout)
    if not allowed:
        log.info("scrape_blocked_by_robots", url=url)
        return ScrapedSite(url=url, status=_STATUS_BLOCKED)

    html, final_url, status_code, err = await _fetch_html(
        client, url, user_agent=user_agent, timeout=timeout, max_bytes=max_bytes
    )
    if err:
        return ScrapedSite(
            url=url,
            status=err,
            final_url=final_url,
            status_code=status_code,
            error=err,
        )

    parser = _SiteParser()
    try:
        parser.feed(html)
    except Exception as exc:  # noqa: BLE001
        log.warning("scrape_parse_failed", url=url, error=str(exc))

    description = _description_from(parser)
    socials = _extract_socials(parser.links, final_url or url)
    about_url = _find_about_link(parser.links, final_url or url) if max_pages > 1 else None
    text = parser.visible_text(max_text)

    about_text = ""
    if about_url:
        about_html, about_final, _sc, about_err = await _fetch_html(
            client,
            about_url,
            user_agent=user_agent,
            timeout=timeout,
            max_bytes=max_bytes,
        )
        if about_err:
            log.debug("scrape_about_fetch_failed", url=about_url, error=about_err)
        elif about_html:
            about_parser = _SiteParser()
            try:
                about_parser.feed(about_html)
            except Exception as exc:  # noqa: BLE001
                log.warning("scrape_about_parse_failed", url=about_url, error=str(exc))
            about_text = about_parser.visible_text(max_text)
            # Promote socials we missed on the homepage
            for k, v in _extract_socials(about_parser.links, about_final or about_url).items():
                socials.setdefault(k, v)

    return ScrapedSite(
        url=url,
        status=_STATUS_OK,
        final_url=final_url,
        title=parser.title.strip() if parser.title else None,
        description=description,
        text=text,
        about_url=about_url,
        about_text=about_text,
        social_links=socials,
        status_code=status_code,
    )


async def scrape_batch(
    urls: list[str | None],
    *,
    concurrency: int | None = None,
) -> dict[str, ScrapedSite]:
    """Scrape many sites in parallel. Returns ``{normalized_url: ScrapedSite}``.

    Empty / unparseable inputs are still represented in the result with a
    ``skipped`` status so the caller can correlate inputs to outcomes by index.
    """
    cfg = get_settings()
    sem_n = concurrency or int(getattr(cfg, "WEBSITE_SCRAPE_CONCURRENCY", 5))
    semaphore = asyncio.Semaphore(sem_n)
    results: dict[str, ScrapedSite] = {}

    async with httpx.AsyncClient() as client:

        async def _one(raw: str | None) -> None:
            normalized = _normalize_url(raw) or (raw or "")
            async with semaphore:
                results[normalized] = await scrape_site(raw, client=client, settings=cfg)

        await asyncio.gather(*(_one(u) for u in urls))

    return results


__all__ = [
    "ScrapedSite",
    "scrape_batch",
    "scrape_site",
]
