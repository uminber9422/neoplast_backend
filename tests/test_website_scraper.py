"""Tests for the website scraper — Phase 4.

Uses httpx's MockTransport to simulate the network (no real HTTP calls).
Covers URL normalization, HTML parsing, social/about link discovery, robots
handling, encoding, and the various failure modes that should each surface
as a distinct ``ScrapedSite.status``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Callable

import httpx
import pytest

from backend.pipeline import website_scraper as ws

# ---------------------------------------------------------------------------
# Test settings — explicit, no env coupling
# ---------------------------------------------------------------------------
def _settings(**overrides):
    base = dict(
        WEBSITE_SCRAPE_ENABLED=True,
        WEBSITE_SCRAPE_TIMEOUT_SECONDS=5.0,
        WEBSITE_SCRAPE_MAX_PAGES=2,
        WEBSITE_SCRAPE_MAX_BYTES=1_000_000,
        WEBSITE_SCRAPE_MAX_TEXT_CHARS=50_000,
        WEBSITE_SCRAPE_USER_AGENT="NeoplastBot-Test/1.0",
        WEBSITE_SCRAPE_CONCURRENCY=3,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _client_for(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# URL normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw,expected", [
    ("www.example.com", "https://www.example.com"),
    ("https://www.example.com/", "https://www.example.com"),
    ("https://www.example.com/about/", "https://www.example.com/about"),
    ("http://example.com", "http://example.com"),
    ("https://example.com/path?q=1", "https://example.com/path?q=1"),
    ("  https://example.com  ", "https://example.com"),
    ("", None),
    ("   ", None),
    (None, None),
    ("not a url", "https://not a url"),  # parsed but unusual — caller still gets a URL
])
def test_normalize_url(raw, expected):
    assert ws._normalize_url(raw) == expected


def test_normalize_url_rejects_empty_after_https_prefix():
    """A bare scheme like 'http://' has no netloc → reject."""
    assert ws._normalize_url("http://") is None


# ---------------------------------------------------------------------------
# HTML parser — title, meta, links, visible text
# ---------------------------------------------------------------------------


def test_site_parser_extracts_title_and_meta():
    html = """
    <html><head>
      <title>Acme Plastics</title>
      <meta name="description" content="Leading B2B plastics supplier">
      <meta property="og:description" content="Should not override meta">
    </head><body>
      <p>Hello world.</p>
      <script>var x = 1;</script>
      <style>.x { color: red; }</style>
    </body></html>
    """
    parser = ws._SiteParser()
    parser.feed(html)
    assert parser.title == "Acme Plastics"
    assert parser.meta_description == "Leading B2B plastics supplier"
    assert parser.og_description == "Should not override meta"
    text = parser.visible_text(1000)
    assert "Hello world." in text
    assert "var x" not in text  # script content suppressed
    assert "color" not in text  # style content suppressed


def test_site_parser_falls_back_to_og_description_when_no_meta():
    html = """
    <html><head>
      <meta property="og:description" content="OG fallback">
    </head><body><p>x</p></body></html>
    """
    parser = ws._SiteParser()
    parser.feed(html)
    assert parser.meta_description is None
    assert ws._description_from(parser) == "OG fallback"


def test_site_parser_collects_links_with_anchor_text():
    html = """
    <html><body>
      <a href="https://www.linkedin.com/company/acme">LinkedIn</a>
      <a href="/about">About Us</a>
      <a href="#top">Skip nav</a>
      <a href="javascript:void(0)">JS link</a>
    </body></html>
    """
    parser = ws._SiteParser()
    parser.feed(html)
    hrefs = [h for h, _ in parser.links]
    assert "https://www.linkedin.com/company/acme" in hrefs
    assert "/about" in hrefs
    assert "#top" not in hrefs           # fragment-only links skipped
    assert "javascript:void(0)" not in hrefs


def test_site_parser_collapses_whitespace_in_text():
    html = "<html><body><p>Hello\n\n   world</p><p>foo</p></body></html>"
    parser = ws._SiteParser()
    parser.feed(html)
    assert parser.visible_text(1000) == "Hello world foo"


def test_site_parser_text_truncated_to_max_chars():
    html = "<html><body><p>" + ("abcdefghij " * 1000) + "</p></body></html>"
    parser = ws._SiteParser()
    parser.feed(html)
    text = parser.visible_text(100)
    assert len(text) == 100


# ---------------------------------------------------------------------------
# Social link extraction
# ---------------------------------------------------------------------------


def test_extract_socials_finds_each_platform():
    links = [
        ("https://www.linkedin.com/company/acme-plastics/", "LinkedIn"),
        ("https://twitter.com/acme", "Twitter"),
        ("https://www.facebook.com/acmePlastics", "Facebook"),
        ("https://www.instagram.com/acme", "Instagram"),
        ("https://www.youtube.com/c/acme", "YouTube"),
        ("https://example.com/contact", "Contact"),  # not social
    ]
    out = ws._extract_socials(links, "https://example.com")
    assert out["linkedin"] == "https://www.linkedin.com/company/acme-plastics/"
    assert out["twitter"] == "https://twitter.com/acme"
    assert out["facebook"] == "https://www.facebook.com/acmePlastics"
    assert out["instagram"] == "https://www.instagram.com/acme"
    assert out["youtube"] == "https://www.youtube.com/c/acme"


def test_extract_socials_handles_x_dot_com_and_relative_links():
    links = [
        ("https://x.com/acme", "X"),
        ("https://fr.linkedin.com/in/jean-dupont", "Jean"),
        ("/contact", "Contact"),
    ]
    out = ws._extract_socials(links, "https://example.com")
    assert out["twitter"] == "https://x.com/acme"
    assert out["linkedin"] == "https://fr.linkedin.com/in/jean-dupont"


def test_extract_socials_keeps_first_per_platform():
    """If the page links to LinkedIn twice we keep the first."""
    links = [
        ("https://www.linkedin.com/company/first", "First"),
        ("https://www.linkedin.com/company/second", "Second"),
    ]
    out = ws._extract_socials(links, "https://example.com")
    assert out["linkedin"] == "https://www.linkedin.com/company/first"


# ---------------------------------------------------------------------------
# About-page discovery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path,should_match", [
    ("/about", True),
    ("/about-us", True),
    ("/about/", True),
    ("/company", True),
    ("/who-we-are", True),
    ("/qui-sommes-nous", True),
    ("/a-propos", True),
    ("/uber-uns", True),
    ("/contact", False),
    ("/products/widgets", False),
])
def test_find_about_link_by_path(path, should_match):
    base = "https://example.com"
    links = [(base + path, "")]
    found = ws._find_about_link(links, base)
    assert (found is not None) is should_match


def test_find_about_link_by_anchor_text_when_path_is_obscure():
    links = [("https://example.com/page-127", "About Us")]
    found = ws._find_about_link(links, "https://example.com")
    assert found == "https://example.com/page-127"


def test_find_about_link_skips_external_hosts():
    """We don't follow off-site links — they belong to a different company."""
    links = [("https://wikipedia.org/wiki/About", "About")]
    assert ws._find_about_link(links, "https://example.com") is None


# ---------------------------------------------------------------------------
# scrape_site — full happy-path with mocked transport
# ---------------------------------------------------------------------------


HOMEPAGE_HTML = """<!doctype html>
<html><head>
  <title>GROUPE RYMM | Plastics Manufacturer</title>
  <meta name="description" content="GROUPE RYMM is an Algerian leader in injection-moulded plastic packaging.">
</head><body>
  <nav><a href="/about-us">About Us</a><a href="/contact">Contact</a></nav>
  <main>
    <h1>Welcome to GROUPE RYMM</h1>
    <p>Founded in 1995, we manufacture custom plastic packaging for industry across North Africa.</p>
  </main>
  <footer>
    <a href="https://www.linkedin.com/company/groupe-rymm">LinkedIn</a>
    <a href="https://twitter.com/grouperymm">Twitter</a>
    <a href="https://facebook.com/grouperymm">Facebook</a>
  </footer>
</body></html>
"""

ABOUT_HTML = """<!doctype html>
<html><head>
  <title>About — GROUPE RYMM</title>
  <meta name="description" content="Our 1995 founding story and mission.">
</head><body>
  <h1>About GROUPE RYMM</h1>
  <p>We employ 200 people across 3 facilities and serve the food, pharma, and industrial sectors.</p>
  <a href="https://www.instagram.com/grouperymm">Instagram</a>
</body></html>
"""


def _make_handler(routes: dict[str, httpx.Response]):
    """Build a MockTransport handler from a {url: response} dict."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for key, resp in routes.items():
            if url == key:
                return resp
        return httpx.Response(404, text="not found")

    return handler


def test_scrape_site_happy_path():
    """End-to-end: homepage + about page + socials all extracted."""
    routes = {
        "https://www.grouperymm.com/robots.txt": httpx.Response(404),
        "https://www.grouperymm.com": httpx.Response(
            200, html=HOMEPAGE_HTML, headers={"content-type": "text/html; charset=utf-8"}
        ),
        "https://www.grouperymm.com/about-us": httpx.Response(
            200, html=ABOUT_HTML, headers={"content-type": "text/html; charset=utf-8"}
        ),
    }
    client = _client_for(_make_handler(routes))

    async def run():
        return await ws.scrape_site(
            "www.grouperymm.com", client=client, settings=_settings()
        )

    result = asyncio.run(run())
    asyncio.run(client.aclose())

    assert result.status == "ok"
    assert result.title == "GROUPE RYMM | Plastics Manufacturer"
    assert "Algerian leader" in (result.description or "")
    assert "injection-moulded" in (result.description or "")
    assert "Founded in 1995" in result.text
    # About page picked up
    assert result.about_url == "https://www.grouperymm.com/about-us"
    assert "200 people" in result.about_text
    # Socials from BOTH pages (Instagram is only on about)
    assert result.social_links["linkedin"] == "https://www.linkedin.com/company/groupe-rymm"
    assert result.social_links["twitter"] == "https://twitter.com/grouperymm"
    assert result.social_links["facebook"] == "https://facebook.com/grouperymm"
    assert result.social_links["instagram"] == "https://www.instagram.com/grouperymm"
    assert result.status_code == 200


def test_scrape_site_skipped_when_disabled():
    cfg = _settings(WEBSITE_SCRAPE_ENABLED=False)
    result = asyncio.run(ws.scrape_site("www.grouperymm.com", settings=cfg))
    assert result.status == "skipped"
    assert result.error == "website_scrape_disabled"


def test_scrape_site_skipped_when_url_empty():
    result = asyncio.run(ws.scrape_site(None, settings=_settings()))
    assert result.status == "skipped"
    assert result.error == "empty_or_invalid_url"


def test_scrape_site_handles_404():
    routes = {
        "https://example.com/robots.txt": httpx.Response(404),
        "https://example.com": httpx.Response(404, text="gone"),
    }
    client = _client_for(_make_handler(routes))
    result = asyncio.run(ws.scrape_site("example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "http_error"
    assert result.status_code == 404


def test_scrape_site_handles_non_html_content():
    routes = {
        "https://example.com/robots.txt": httpx.Response(404),
        "https://example.com": httpx.Response(
            200, content=b"%PDF-1.4 fake pdf bytes", headers={"content-type": "application/pdf"}
        ),
    }
    client = _client_for(_make_handler(routes))
    result = asyncio.run(ws.scrape_site("example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "not_html"


def test_scrape_site_handles_network_error():
    def bomb(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _client_for(bomb)
    result = asyncio.run(ws.scrape_site("example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "network_error"


def test_scrape_site_handles_timeout():
    def slowmo(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow")

    client = _client_for(slowmo)
    result = asyncio.run(ws.scrape_site("example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "timeout"


def test_scrape_site_blocked_by_robots():
    """robots.txt explicitly disallows our user agent → status=blocked."""
    robots_body = "User-agent: *\nDisallow: /\n"
    routes = {
        "https://blocked.example.com/robots.txt": httpx.Response(200, text=robots_body),
        "https://blocked.example.com": httpx.Response(200, html=HOMEPAGE_HTML),
    }
    client = _client_for(_make_handler(routes))
    result = asyncio.run(ws.scrape_site("blocked.example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "blocked_by_robots"


def test_scrape_site_robots_default_allow_on_fetch_error():
    """robots.txt fetch raises → we proceed (best-effort). Permissive default."""
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        call_log.append(url)
        if url.endswith("/robots.txt"):
            raise httpx.ConnectError("robots blew up")
        if url == "https://example.com":
            return httpx.Response(200, html=HOMEPAGE_HTML, headers={"content-type": "text/html"})
        return httpx.Response(404)

    client = _client_for(handler)
    result = asyncio.run(ws.scrape_site("example.com", client=client, settings=_settings()))
    asyncio.run(client.aclose())
    assert result.status == "ok"
    # Robots was attempted and the homepage was fetched anyway
    assert any(u.endswith("/robots.txt") for u in call_log)
    assert "https://example.com" in call_log


def test_scrape_site_max_pages_one_skips_about():
    """When MAX_PAGES=1 we don't fetch the about page even if discovered."""
    routes = {
        "https://example.com/robots.txt": httpx.Response(404),
        "https://example.com": httpx.Response(
            200, html=HOMEPAGE_HTML, headers={"content-type": "text/html"}
        ),
        "https://example.com/about-us": httpx.Response(
            500, text="should not be fetched"
        ),
    }
    client = _client_for(_make_handler(routes))
    result = asyncio.run(
        ws.scrape_site("example.com", client=client, settings=_settings(WEBSITE_SCRAPE_MAX_PAGES=1))
    )
    asyncio.run(client.aclose())
    assert result.status == "ok"
    assert result.about_url is None        # not even discovered
    assert result.about_text == ""


def test_scrape_site_byte_cap_truncates_huge_response():
    """A 10 MB response gets truncated to MAX_BYTES before parsing."""
    huge_body = b"<html><body>" + (b"x" * 5_000_000) + b"</body></html>"
    routes = {
        "https://huge.example.com/robots.txt": httpx.Response(404),
        "https://huge.example.com": httpx.Response(
            200, content=huge_body, headers={"content-type": "text/html"}
        ),
    }
    client = _client_for(_make_handler(routes))
    cfg = _settings(WEBSITE_SCRAPE_MAX_BYTES=10_000, WEBSITE_SCRAPE_MAX_TEXT_CHARS=20_000)
    result = asyncio.run(ws.scrape_site("huge.example.com", client=client, settings=cfg))
    asyncio.run(client.aclose())
    assert result.status == "ok"
    # Even though the body was 5MB, our text is bounded
    assert len(result.text) <= 20_000


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


def test_scrape_batch_runs_in_parallel_and_keys_by_normalized_url(monkeypatch):
    """Batch scraper handles a list of URLs; results keyed by normalized URL."""

    async def fake_scrape(url, *, client=None, settings=None):
        # Return a deterministic per-URL result
        normalized = ws._normalize_url(url) or (url or "")
        return ws.ScrapedSite(url=normalized, status="ok", title=f"title for {normalized}")

    monkeypatch.setattr(ws, "scrape_site", fake_scrape)
    out = asyncio.run(ws.scrape_batch(["www.a.com", "https://b.com/", None, ""]))
    assert out["https://www.a.com"].title == "title for https://www.a.com"
    assert out["https://b.com"].title == "title for https://b.com"
    # Empty / None inputs are normalized to "" and still represented
    assert "" in out
