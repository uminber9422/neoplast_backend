"""Per-pipeline-run structured logger.

Creates a JSONL (one JSON object per line) log file for every pipeline run,
capturing the full request/response cycle of every external API call
(ZeroBounce, Serper, OpenAI) so the operator can trace exactly what happened.

Log files are stored under  ``data/logs/run_{id}_{timestamp}.jsonl``.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from backend.config import LOGS_DIR

# Keys whose *values* must never be written to disk.
_REDACT_KEYS = frozenset(
    {
        "api_key",
        "x-api-key",
        "authorization",
        "token",
        "access_token",
        "secret",
        "secret_key",
        "zerobounce_api_key",
        "reoon_api_key",
        "serper_api_key",
        "openai_api_key",
    }
)


def _redact(obj: Any, *, depth: int = 0) -> Any:
    """Recursively mask sensitive values in dicts/lists before writing."""
    if depth > 10:
        return obj
    if isinstance(obj, dict):
        return {
            k: "***REDACTED***" if k.lower() in _REDACT_KEYS else _redact(v, depth=depth + 1)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return [_redact(v, depth=depth + 1) for v in obj]
    return obj


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunLogger:
    """Append-only JSONL logger scoped to a single pipeline run.

    Usage::

        rl = RunLogger(run_id=42)
        rl.log("zerobounce_request", email="a@b.com", request={...})
        rl.log("zerobounce_response", email="a@b.com", response={...}, duration_ms=350)
    """

    def __init__(self, run_id: int) -> None:
        self.run_id = run_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self.path = LOGS_DIR / f"run_{run_id}_{ts}.jsonl"
        # Create the file immediately so its existence is visible.
        self.path.touch(exist_ok=True)

    @property
    def filename(self) -> str:
        return self.path.name

    # ------------------------------------------------------------------
    # Core write
    # ------------------------------------------------------------------
    def log(self, event_type: str, **fields: Any) -> None:
        """Append one JSON-Lines entry."""
        entry: dict[str, Any] = {
            "ts": _now_iso(),
            "run_id": self.run_id,
            "event": event_type,
        }
        entry.update(_redact(fields))
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    def stage_start(self, stage: str, **extra: Any) -> None:
        self.log("stage_start", stage=stage, **extra)

    def stage_end(self, stage: str, **extra: Any) -> None:
        self.log("stage_end", stage=stage, **extra)

    def error(self, stage: str, message: str, **extra: Any) -> None:
        self.log("error", stage=stage, message=message[:2000], **extra)

    @contextmanager
    def timed(self, event_type: str, **fields: Any) -> Generator[dict, None, None]:
        """Context manager that auto-logs duration_ms on exit.

        Yields a dict where the caller can stash ``response`` or other data
        that should be included in the final log entry.
        """
        t0 = time.perf_counter()
        extra: dict[str, Any] = {}
        try:
            yield extra
        except Exception as exc:
            extra["error"] = str(exc)[:2000]
            raise
        finally:
            extra["duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            self.log(event_type, **fields, **extra)

    # ------------------------------------------------------------------
    # Read helpers (for the API to serve logs)
    # ------------------------------------------------------------------
    def read_entries(
        self,
        *,
        stage: str | None = None,
        email: str | None = None,
    ) -> list[dict]:
        """Read all log entries, optionally filtered."""
        if not self.path.exists():
            return []
        entries: list[dict] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if stage and entry.get("stage") != stage:
                continue
            if email and entry.get("email") != email:
                continue
            entries.append(entry)
        return entries


def read_run_log(
    run_id: int,
    *,
    stage: str | None = None,
    email: str | None = None,
) -> list[dict]:
    """Read log entries for a run by scanning LOGS_DIR for matching file."""
    if not LOGS_DIR.exists():
        return []
    for p in sorted(LOGS_DIR.glob(f"run_{run_id}_*.jsonl")):
        entries: list[dict] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if stage and entry.get("stage") != stage:
                continue
            if email and entry.get("email") != email:
                continue
            entries.append(entry)
        return entries
    return []


__all__ = ["RunLogger", "read_run_log"]
