"""Security response headers middleware.

Sets a baseline of headers protecting against common web vulnerabilities:
clickjacking, MIME sniffing, referrer leakage, and XSS via inline scripts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Apply security-related response headers."""

    def __init__(self, app, *, csp: str | None = None) -> None:
        super().__init__(app)
        # Strict CSP: only same-origin scripts/styles, no inline by default.
        # Vite dev server needs inline styles, so dev users override via param.
        self.csp = csp or (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: blob:; "
            "font-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'"
        )

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        # Swagger UI uses inline scripts + CDN assets; relax CSP for /api/docs
        # only. This path is disabled in production.
        if request.url.path.startswith("/api/docs") or request.url.path.startswith(
            "/api/redoc"
        ):
            response.headers["Content-Security-Policy"] = (
                "default-src 'self' cdn.jsdelivr.net; "
                "script-src 'self' 'unsafe-inline' cdn.jsdelivr.net; "
                "style-src 'self' 'unsafe-inline' cdn.jsdelivr.net; "
                "img-src 'self' data: cdn.jsdelivr.net fastapi.tiangolo.com; "
                "frame-ancestors 'none'"
            )
        else:
            response.headers["Content-Security-Policy"] = self.csp
        # HSTS only meaningful behind HTTPS; harmless on HTTP but noisy.
        # Enable when deploying with TLS.
        # response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response
