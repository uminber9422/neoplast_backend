"""Data profiler — derive country / locale / language from a prospect's signals.

For each prospect we look at three independent sources and combine them:

    1. Explicit ``country`` column (from CSV; may be localized: "Algerie")
    2. Email ccTLD               (e.g. ``…@itp.dz`` → Algeria)
    3. Phone country prefix      (e.g. ``+213…`` or ``00213…`` → Algeria)

Signals are weighted (CSV > phone > email) and combined; agreeing signals
push confidence higher. The resulting :class:`ProspectProfile` feeds the
geo-aware web search (Phase 5) and the locale-aware LLM prompt (Phase 6).

Pure-stdlib, no external deps. Self-contained — no DB or network access.
This module deliberately does not write to the DB; the orchestrator wiring
in Phase 7 will call :func:`apply_profile_to_prospect` to persist.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Country reference data — single source of truth.
# ---------------------------------------------------------------------------
# (alpha2, name, e164_calling_code, primary_business_language)
#
# "Primary business language" = what's most useful for B2B web search and LLM
# context, which is not always the official language. Algeria is the headline
# example: official language is Arabic, but business / industry pages are
# overwhelmingly French, so we tag it ``fr``.
_COUNTRY_TABLE: tuple[tuple[str, str, str, str], ...] = (
    ("AE", "United Arab Emirates", "971", "en"),
    ("AR", "Argentina", "54", "es"),
    ("AT", "Austria", "43", "de"),
    ("AU", "Australia", "61", "en"),
    ("BD", "Bangladesh", "880", "en"),
    ("BE", "Belgium", "32", "fr"),
    ("BR", "Brazil", "55", "pt"),
    ("CA", "Canada", "1", "en"),
    ("CH", "Switzerland", "41", "de"),
    ("CL", "Chile", "56", "es"),
    ("CN", "China", "86", "zh"),
    ("CO", "Colombia", "57", "es"),
    ("CZ", "Czechia", "420", "cs"),
    ("DE", "Germany", "49", "de"),
    ("DK", "Denmark", "45", "da"),
    ("DZ", "Algeria", "213", "fr"),
    ("EG", "Egypt", "20", "ar"),
    ("ES", "Spain", "34", "es"),
    ("FI", "Finland", "358", "fi"),
    ("FR", "France", "33", "fr"),
    ("GB", "United Kingdom", "44", "en"),
    ("GR", "Greece", "30", "el"),
    ("HK", "Hong Kong", "852", "en"),
    ("ID", "Indonesia", "62", "id"),
    ("IE", "Ireland", "353", "en"),
    ("IL", "Israel", "972", "he"),
    ("IN", "India", "91", "en"),
    ("IR", "Iran", "98", "fa"),
    ("IT", "Italy", "39", "it"),
    ("JO", "Jordan", "962", "ar"),
    ("JP", "Japan", "81", "ja"),
    ("KE", "Kenya", "254", "en"),
    ("KR", "South Korea", "82", "ko"),
    ("KW", "Kuwait", "965", "ar"),
    ("LB", "Lebanon", "961", "ar"),
    ("LK", "Sri Lanka", "94", "en"),
    ("MA", "Morocco", "212", "fr"),
    ("MX", "Mexico", "52", "es"),
    ("MY", "Malaysia", "60", "en"),
    ("NG", "Nigeria", "234", "en"),
    ("NL", "Netherlands", "31", "nl"),
    ("NO", "Norway", "47", "no"),
    ("NZ", "New Zealand", "64", "en"),
    ("OM", "Oman", "968", "ar"),
    ("PE", "Peru", "51", "es"),
    ("PH", "Philippines", "63", "en"),
    ("PK", "Pakistan", "92", "en"),
    ("PL", "Poland", "48", "pl"),
    ("PT", "Portugal", "351", "pt"),
    ("QA", "Qatar", "974", "ar"),
    ("RO", "Romania", "40", "ro"),
    ("RU", "Russia", "7", "ru"),
    ("SA", "Saudi Arabia", "966", "ar"),
    ("SE", "Sweden", "46", "sv"),
    ("SG", "Singapore", "65", "en"),
    ("TH", "Thailand", "66", "th"),
    ("TN", "Tunisia", "216", "fr"),
    ("TR", "Turkey", "90", "tr"),
    ("TW", "Taiwan", "886", "zh"),
    ("UA", "Ukraine", "380", "uk"),
    ("US", "United States", "1", "en"),
    ("VN", "Vietnam", "84", "vi"),
    ("ZA", "South Africa", "27", "en"),
)

# Canonical lookup: alpha2 → (name, calling_code, language).
_BY_CODE: dict[str, tuple[str, str, str]] = {
    code: (name, cc, lang) for code, name, cc, lang in _COUNTRY_TABLE
}

# Calling-code → alpha2, sorted by code length DESC for greedy longest match.
_CALLING_CODES: list[tuple[str, str]] = sorted(
    [(cc, code) for code, _, cc, _ in _COUNTRY_TABLE],
    key=lambda x: -len(x[0]),
)

# When two countries share a calling code, pick a primary so phone matching
# stays deterministic. (US/CA both = +1; RU/KZ both = +7, etc.)
_PRIMARY_FOR_CALLING_CODE: dict[str, str] = {
    "1": "US",
    "7": "RU",
}

# ccTLD → alpha2. Most ccTLDs match the alpha2 lowercased; the .uk override
# keeps that mapping explicit even though the alpha2 is GB.
_CCTLDS: dict[str, str] = {code.lower(): code for code, _, _, _ in _COUNTRY_TABLE}
_CCTLDS["uk"] = "GB"

# Generic / multinational TLDs that carry no geo signal.
_GENERIC_TLDS: frozenset[str] = frozenset(
    {
        "com", "net", "org", "info", "biz", "io", "co", "ai", "app",
        "dev", "edu", "gov", "mil", "int", "name", "pro", "tech",
        "online", "store", "site", "xyz", "global", "world", "cloud",
    }
)

# Country-name aliases (lowercased, diacritics stripped) → alpha2. Only map
# to codes present in `_COUNTRY_TABLE` — the lookup blindly trusts these.
_NAME_ALIASES: dict[str, str] = {
    # Algeria
    "algerie": "DZ",
    "al-jazair": "DZ",
    "aljazair": "DZ",
    "el djazair": "DZ",
    # India
    "bharat": "IN",
    "republic of india": "IN",
    # USA
    "usa": "US",
    "u.s.a.": "US",
    "u.s.": "US",
    "us": "US",
    "united states of america": "US",
    "america": "US",
    # UK
    "uk": "GB",
    "u.k.": "GB",
    "great britain": "GB",
    "britain": "GB",
    "england": "GB",
    # Other common name variants
    "deutschland": "DE",
    "espana": "ES",
    "korea": "KR",
    "south korea": "KR",
    "republic of korea": "KR",
    "russia": "RU",
    "russian federation": "RU",
    "uae": "AE",
    "u.a.e.": "AE",
    "ksa": "SA",
    "viet nam": "VN",
    "czech republic": "CZ",
    "holland": "NL",
    "iran, islamic republic of": "IR",
}

# Diacritic stripping for country names (Algérie → algerie, España → espana).
_DIACRITIC_MAP = str.maketrans(
    {
        "á": "a", "à": "a", "â": "a", "ä": "a", "ã": "a", "å": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "í": "i", "ì": "i", "î": "i", "ï": "i",
        "ó": "o", "ò": "o", "ô": "o", "ö": "o", "õ": "o",
        "ú": "u", "ù": "u", "û": "u", "ü": "u",
        "ñ": "n", "ç": "c", "ß": "ss",
        "Á": "A", "À": "A", "Â": "A", "Ä": "A",
        "É": "E", "È": "E", "Ê": "E", "Ë": "E",
        "Í": "I", "Ì": "I", "Î": "I", "Ï": "I",
        "Ó": "O", "Ò": "O", "Ô": "O", "Ö": "O",
        "Ú": "U", "Ù": "U", "Û": "U", "Ü": "U",
        "Ñ": "N", "Ç": "C",
    }
)


def _strip_diacritics(s: str) -> str:
    return s.translate(_DIACRITIC_MAP)


# Build canonical name → alpha2 from the table, plus aliases.
_NAME_TO_CODE: dict[str, str] = {}
for _code, _name, _cc, _lang in _COUNTRY_TABLE:
    _NAME_TO_CODE[_name.lower()] = _code
    _NAME_TO_CODE[_strip_diacritics(_name).lower()] = _code
_NAME_TO_CODE.update(_NAME_ALIASES)


# Confidence weights per signal source. Sum > 1.0 so that two agreeing signals
# saturate to ``1.0`` (we cap the final value).
_WEIGHT_CSV_COUNTRY = 0.6
_WEIGHT_PHONE = 0.4
_WEIGHT_EMAIL_TLD = 0.3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@dataclass
class ProspectProfile:
    """Geographic / linguistic profile derived from a prospect's signals.

    All fields default to ``None`` when no signal resolves. ``signals`` and
    ``confidence`` expose the reasoning so the pipeline UI / logs can show
    *why* the detection was made.
    """

    country: str | None = None              # "Algeria"
    country_code: str | None = None         # "DZ"
    search_locale: str | None = None        # "dz" — Serper `gl` parameter
    language: str | None = None             # "fr" — Serper `hl` parameter
    signals: list[str] = field(default_factory=list)
    confidence: float = 0.0


def profile_prospect(
    *,
    email: str | None = None,
    phone: str | None = None,
    country: str | None = None,
) -> ProspectProfile:
    """Derive a :class:`ProspectProfile` from raw prospect signals.

    Sources are checked independently and combined with weighted voting:
        - CSV ``country``  (weight 0.6)
        - phone prefix     (weight 0.4)
        - email ccTLD      (weight 0.3)

    A signal contributes only if it resolves to a country in our table. Two
    agreeing signals reach ``confidence = 1.0``; CSV alone reaches ``0.6``.
    Conflicts are resolved by total weight, so CSV beats phone, and any
    combination of two agreeing weaker signals beats CSV alone.
    """
    signals: list[str] = []
    votes: list[tuple[str, float]] = []

    if country:
        code = _country_name_to_code(country)
        cleaned = country.strip()
        if code:
            votes.append((code, _WEIGHT_CSV_COUNTRY))
            signals.append(f"csv_country='{cleaned}'->{code}")
        else:
            signals.append(f"csv_country='{cleaned}'->unknown")

    if phone:
        code = _phone_to_country_code(phone)
        if code:
            votes.append((code, _WEIGHT_PHONE))
            signals.append(f"phone_prefix='{phone[:6]}'->{code}")

    if email:
        tld = _extract_email_tld(email)
        code = _CCTLDS.get(tld) if tld and tld not in _GENERIC_TLDS else None
        if code:
            votes.append((code, _WEIGHT_EMAIL_TLD))
            signals.append(f"email_tld='.{tld}'->{code}")
        elif tld and tld in _GENERIC_TLDS:
            signals.append(f"email_tld='.{tld}'->generic(ignored)")

    if not votes:
        return ProspectProfile(signals=signals)

    tally: dict[str, float] = {}
    for code, weight in votes:
        tally[code] = tally.get(code, 0.0) + weight
    winning_code = max(tally, key=lambda k: tally[k])
    confidence = min(tally[winning_code], 1.0)

    name, _cc, language = _BY_CODE[winning_code]
    return ProspectProfile(
        country=name,
        country_code=winning_code,
        search_locale=winning_code.lower(),
        language=language,
        signals=signals,
        confidence=round(confidence, 3),
    )


def profile_prospect_obj(prospect: Any) -> ProspectProfile:
    """Convenience wrapper that takes a Prospect-like object (or dict)."""
    if isinstance(prospect, dict):
        return profile_prospect(
            email=prospect.get("email"),
            phone=prospect.get("phone"),
            country=prospect.get("country"),
        )
    return profile_prospect(
        email=getattr(prospect, "email", None),
        phone=getattr(prospect, "phone", None),
        country=getattr(prospect, "country", None),
    )


def apply_profile_to_prospect(prospect: Any, profile: ProspectProfile) -> None:
    """Persist a :class:`ProspectProfile` onto a Prospect ORM object.

    Writes ``detected_country_code`` and ``search_locale`` unconditionally
    (they're our derived fields). ``country`` is filled only when the prospect
    didn't already have one from the CSV — we never overwrite the CSV value
    even when the profiler couldn't normalize it.
    """
    if profile.country and not getattr(prospect, "country", None):
        prospect.country = profile.country
    prospect.detected_country_code = profile.country_code
    prospect.search_locale = profile.search_locale


def language_for_country_code(country_code: str | None) -> str | None:
    """Look up the primary business language for an ISO alpha-2 country code."""
    if not country_code:
        return None
    info = _BY_CODE.get(country_code.upper())
    if not info:
        return None
    return info[2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_NON_DIGIT_RE = re.compile(r"\D+")


def _country_name_to_code(raw: str) -> str | None:
    """Resolve a free-text country string to an ISO alpha-2 code.

    Tries (in order): exact lowercase match, diacritic-stripped match, alias
    match, and finally a 2-letter ISO code passed straight through.
    """
    if not raw:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    key = _strip_diacritics(cleaned.lower())
    code = _NAME_TO_CODE.get(key)
    if code:
        return code
    upper = cleaned.upper()
    if len(upper) == 2 and upper in _BY_CODE:
        return upper
    return None


def _phone_to_country_code(raw: str) -> str | None:
    """Match a phone with international prefix (``+`` or ``00``) to alpha-2.

    Returns ``None`` for unprefixed local numbers (the function refuses to
    guess from length alone). One caveat the caller should know:
    :func:`backend.utils.phone.normalize_phone` *prepends* ``+91`` to bare
    10-digit numbers as an India default, so a phone arriving here as
    ``+91XXXXXXXXXX`` may originate from a non-Indian local number whose true
    country is unknown. The profiler weights the phone signal at only 0.4
    precisely so a stronger CSV/email signal can override it.
    """
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("+"):
        digits = _NON_DIGIT_RE.sub("", text)
    else:
        digits = _NON_DIGIT_RE.sub("", text)
        if digits.startswith("00"):
            digits = digits[2:]
        else:
            return None
    if not digits:
        return None
    for code_str, alpha2 in _CALLING_CODES:
        if digits.startswith(code_str):
            return _PRIMARY_FOR_CALLING_CODE.get(code_str, alpha2)
    return None


def _extract_email_tld(email: str) -> str | None:
    """Return the lowercase TLD of an email address, or None if unparseable."""
    if not email or "@" not in email:
        return None
    domain = email.rsplit("@", 1)[-1].strip().lower()
    if not domain or "." not in domain:
        return None
    return domain.rsplit(".", 1)[-1]


__all__ = [
    "ProspectProfile",
    "apply_profile_to_prospect",
    "language_for_country_code",
    "profile_prospect",
    "profile_prospect_obj",
]
