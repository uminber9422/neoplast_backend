"""FastAPI application entry point.

Wires up: routers, middleware (CORS, security headers, trusted hosts,
rate limiting), exception handlers, and (in prod) static frontend serving.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend import __version__
from backend.auth.routes import router as auth_router
import os
from backend.config import PROJECT_ROOT, ensure_runtime_dirs, get_settings
from backend.logging_setup import configure_logging, get_logger
from backend.middleware.security_headers import SecurityHeadersMiddleware
from backend.models.database import SessionLocal, init_db
from backend.models.user import User
from backend.auth.security import hash_password


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """App startup / shutdown hooks."""
    settings = get_settings()
    configure_logging(level=settings.LOG_LEVEL, json_logs=settings.is_production)
    log = get_logger(__name__)
    ensure_runtime_dirs()
    
    # Ensure default admin user is seeded on initial setup from ENV
    default_user = os.environ.get("DEFAULT_ADMIN_USER")
    default_pass = os.environ.get("DEFAULT_ADMIN_PASSWORD")
    if default_user and default_pass:
        init_db()
        try:
            with SessionLocal() as db:
                if not db.query(User).filter(User.username == default_user).first():
                    db.add(User(
                        username=default_user,
                        password_hash=hash_password(default_pass),
                        role="admin"
                    ))
                    db.commit()
                    log.info("seeded_default_admin", username=default_user)
        except Exception as e:
            log.error("failed_to_seed_admin", error=str(e))
    log.info(
        "startup",
        env=settings.APP_ENV,
        version=__version__,
        host=settings.APP_HOST,
        port=settings.APP_PORT,
    )
    yield
    log.info("shutdown")


def create_app() -> FastAPI:
    settings = get_settings()
    # Disable interactive docs in production: they'd require relaxing CSP
    # and they're a dev convenience. /api/openapi.json also off in prod.
    docs_url = None if settings.is_production else "/api/docs"
    redoc_url = None if settings.is_production else "/api/redoc"
    openapi_url = None if settings.is_production else "/api/openapi.json"
    app = FastAPI(
        title="Neoplast Lead Dashboard API",
        version=__version__,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        lifespan=lifespan,
    )

    # --- Security middleware (order: outer to inner) ------------------------
    # Trusted host check runs first to reject Host-header attacks.
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.allowed_hosts_list + ["*"]
        if not settings.is_production
        else settings.allowed_hosts_list,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["X-Request-ID"],
    )
    # CSP relaxed for /api/docs (Swagger UI uses inline scripts).
    app.add_middleware(SecurityHeadersMiddleware)

    # --- Routers ------------------------------------------------------------
    app.include_router(auth_router)

    # Lazy import to avoid circular deps with future routers.
    from backend.routes import (
        clusters as clusters_router,
        dashboard as dashboard_router,
        pipeline as pipeline_router,
        prospects as prospects_router,
        settings as settings_router,
        uploads as uploads_router,
    )

    app.include_router(dashboard_router.router)
    app.include_router(prospects_router.router)
    app.include_router(clusters_router.router)
    app.include_router(uploads_router.router)
    app.include_router(pipeline_router.router)
    app.include_router(settings_router.router)

    # --- Health check (unauthenticated) ------------------------------------
    @app.get("/api/health", tags=["health"])
    async def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "env": settings.APP_ENV}

    # --- Generic exception handler -----------------------------------------
    log = get_logger(__name__)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_exception", path=request.url.path, error=str(exc))
        # Never leak internals in the response body.
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    # --- Static frontend (production build) --------------------------------
    frontend_dist = PROJECT_ROOT / "frontend" / "dist"
    if frontend_dist.is_dir():
        app.mount(
            "/",
            StaticFiles(directory=str(frontend_dist), html=True),
            name="frontend",
        )

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "backend.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=not settings.is_production,
    )
