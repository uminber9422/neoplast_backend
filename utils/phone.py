"""Phone number normalization (India-first, falls back to international format)."""

from __future__ import annotations

import re

_NON_DIGIT = re.compile(r"\D+")


def normalize_phone(raw: str | None) -> str | None:
    """Normalize a phone string. Returns None for empty / clearly invalid input.

    Heuristic: strip non-digits, strip a leading '0', prepend '+91' for 10-digit
    Indian numbers. Other lengths return as best-effort '+'+digits.
    """
    if not raw:
        return None
    digits = _NON_DIGIT.sub("", raw)
    if not digits:
        return None

    # Drop common prefixes
    if digits.startswith("00"):
        digits = digits[2:]
    elif digits.startswith("0"):
        digits = digits[1:]

    # Pure 10-digit Indian mobile/landline
    if len(digits) == 10:
        return f"+91{digits}"
    # Already has country code (12 digits incl. India 91)
    if len(digits) == 12 and digits.startswith("91"):
        return f"+{digits}"
    # Anything else: keep digits but prefix '+' so it parses as intl format
    if 7 <= len(digits) <= 15:
        return f"+{digits}"
    return None
