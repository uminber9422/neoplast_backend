"""Rate limiting — simple in-memory sliding window keyed by client IP.

Used as a FastAPI Depends() on auth endpoints to throttle brute-force attacks.
A single-process design is acceptable for the LAN deployment target; switch to
a Redis-backed limiter (e.g. slowapi) if we ever add multi-worker uvicorn.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Callable

from fastapi import HTTPException, Request, status


class SlidingWindowRateLimiter:
    """Thread-safe O(1) sliding-window limiter."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max = max_requests
        self.window = window_seconds
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def hit(self, key: str) -> bool:
        """Record a hit. Returns True if allowed, False if over limit."""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets[key]
            cutoff = now - self.window
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= self.max:
                return False
            bucket.append(now)
            return True


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def parse_rate(spec: str) -> tuple[int, float]:
    """Parse a 'N/period' spec. Period: minute|hour|second."""
    n_str, period = spec.split("/")
    n = int(n_str.strip())
    period = period.strip().lower()
    seconds = {"second": 1, "minute": 60, "hour": 3600}.get(period)
    if seconds is None:
        raise ValueError(f"Invalid rate spec: {spec!r}")
    return n, float(seconds)


def make_rate_limit_dep(spec: str) -> Callable[[Request], None]:
    """Build a FastAPI dependency that enforces `spec` per client IP."""
    max_requests, window = parse_rate(spec)
    limiter = SlidingWindowRateLimiter(max_requests, window)

    def _dep(request: Request) -> None:
        if not limiter.hit(_client_ip(request)):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests. Try again shortly.",
            )

    return _dep
