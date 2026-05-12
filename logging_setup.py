"""Structured logging configuration.

Uses structlog for structured JSON logs in production and human-readable
console output in development. Sensitive keys are redacted automatically.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

# Keys whose values must NEVER appear in logs.
SENSITIVE_KEYS = frozenset(
    {
        "password",
        "password_hash",
        "secret",
        "secret_key",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "authorization",
        "cookie",
        "set-cookie",
        "zerobounce_api_key",
        "reoon_api_key",
        "serper_api_key",
        "openai_api_key",
    }
)


def _redact_sensitive(_logger: Any, _name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """structlog processor that masks sensitive values."""
    for key in list(event_dict.keys()):
        if key.lower() in SENSITIVE_KEYS:
            event_dict[key] = "***REDACTED***"
    return event_dict


def configure_logging(level: str = "INFO", json_logs: bool = False) -> None:
    """Configure structlog and stdlib logging.

    Args:
        level: log level name (DEBUG, INFO, WARNING, ERROR).
        json_logs: emit JSON-formatted logs (use in production).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _redact_sensitive,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_logs:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Pipe stdlib logs (uvicorn, sqlalchemy, etc.) through the same handler.
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)

    # Tame noisy libraries.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str | None = None):
    """Get a configured logger."""
    return structlog.get_logger(name)
