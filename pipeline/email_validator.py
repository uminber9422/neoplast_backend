"""Email validation via ZeroBounce REST API (or mock when key absent).

Single-record endpoint used in batches of N prospects with bounded concurrency.
For production-grade batch validation at high volume, switch to the bulk
`sendfile` endpoint. For ~5k records, sequential w/ retry is plenty.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.config import get_settings
from backend.logging_setup import get_logger

log = get_logger(__name__)

ZEROBOUNCE_URL = "https://api.zerobounce.net/v2/validate"

# Map ZeroBounce statuses to our simplified set.
STATUS_MAP = {
    "valid": "valid",
    "invalid": "invalid",
    "catch-all": "catch-all",
    "spamtrap": "invalid",
    "abuse": "invalid",
    "do_not_mail": "invalid",
    "unknown": "unknown",
}

# Derive email activity from the *original* ZeroBounce status.
# Replaces the old `did_you_mean` heuristic (which over-marked spamtrap/abuse
# emails as "active"). Migration 0003_add_log_file_fix_activity backfills the
# bad rows once. See backend/alembic/versions/0003_*.py.
ACTIVITY_MAP = {
    "valid": "active",
    "invalid": "inactive",
    "catch-all": "unknown",
    "spamtrap": "inactive",
    "abuse": "inactive",
    "do_not_mail": "inactive",
    "unknown": "unknown",
}


@dataclass
class EmailValidationResult:
    status: str
    sub_status: str | None
    activity: str | None
    activity_score: float | None
    # Raw ZeroBounce response — captured for the per-run JSONL trace.
    raw_response: dict | None = None


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
async def _validate_one(
    client: httpx.AsyncClient,
    api_key: str,
    email: str,
    run_logger=None,
) -> EmailValidationResult:
    params = {"api_key": api_key, "email": email, "ip_address": ""}
    if run_logger:
        run_logger.log(
            "zerobounce_request",
            stage="email_validation",
            email=email,
            request={"url": ZEROBOUNCE_URL, "params": params},
        )

    response = await client.get(ZEROBOUNCE_URL, params=params, timeout=15.0)
    response.raise_for_status()
    data = response.json()

    if run_logger:
        run_logger.log(
            "zerobounce_response",
            stage="email_validation",
            email=email,
            response=data,
            http_status=response.status_code,
        )

    original_status = data.get("status", "unknown")
    return EmailValidationResult(
        status=STATUS_MAP.get(original_status, "unknown"),
        sub_status=data.get("sub_status") or None,
        activity=ACTIVITY_MAP.get(original_status, "unknown"),
        activity_score=data.get("activity_score") if data.get("activity_score") is not None else None,
        raw_response=data,
    )


def _mock_validate(email: str) -> EmailValidationResult:
    """Deterministic-ish mock: derives status from the email string itself.

    Used when no API key is configured so the pipeline can be exercised end-to-end
    on a developer machine without spending real API credits.
    """
    h = sum(ord(c) for c in email) % 10
    if h < 6:
        status = "valid"
        activity = "active"
        score = round(0.7 + (h / 30), 2)
    elif h < 8:
        status = "catch-all"
        activity = "unknown"
        score = 0.5
    else:
        status = "invalid"
        activity = "inactive"
        score = 0.0
    return EmailValidationResult(
        status=status,
        sub_status="mock_mode",
        activity=activity,
        activity_score=score,
        raw_response={"mock": True, "status": status},
    )


async def validate_emails(
    emails: list[str],
    *,
    concurrency: int = 5,
    run_logger=None,
) -> dict[str, EmailValidationResult]:
    """Validate a batch of emails. Returns {email: result}. Errors logged not raised."""
    settings = get_settings()
    api_key = settings.ZEROBOUNCE_API_KEY

    if not api_key:
        log.warning("email_validation_mock_mode", count=len(emails))
        if run_logger:
            run_logger.log(
                "zerobounce_mock_mode",
                stage="email_validation",
                count=len(emails),
                message="No API key configured — using mock validation",
            )
        # Tiny delay so callers' progress UI updates feel real.
        await asyncio.sleep(0.05 * len(emails) / max(concurrency, 1))
        return {e: _mock_validate(e) for e in emails}

    results: dict[str, EmailValidationResult] = {}
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:

        async def _bounded(email: str) -> None:
            async with semaphore:
                try:
                    results[email] = await _validate_one(
                        client, api_key, email, run_logger=run_logger,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.error("email_validation_failed", email=email, error=str(exc))
                    if run_logger:
                        run_logger.error(
                            "email_validation",
                            f"Validation failed for {email}: {exc}",
                            email=email,
                        )
                    results[email] = EmailValidationResult(
                        status="unknown",
                        sub_status="api_error",
                        activity="unknown",
                        activity_score=None,
                        raw_response={"error": str(exc)},
                    )

        await asyncio.gather(*(_bounded(e) for e in emails))

    return results


def to_db_fields(result: EmailValidationResult) -> dict:
    """Adapter from validation result to ORM column values."""
    return {
        "email_status": result.status,
        "email_sub_status": result.sub_status,
        "email_activity": result.activity,
        "email_activity_score": result.activity_score,
        "email_validated_at": datetime.now(timezone.utc),
    }


__all__ = ["EmailValidationResult", "to_db_fields", "validate_emails"]
