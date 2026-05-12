"""Application configuration loaded from environment variables.

All secrets and runtime tunables come from `.env` (copy `.env.example`).
Never hardcode values here that should be configurable per environment.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
EXPORTS_DIR = DATA_DIR / "exports"
BACKUPS_DIR = DATA_DIR / "backups"
LOGS_DIR = DATA_DIR / "logs"


class Settings(BaseSettings):
    """Strongly-typed runtime configuration."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- App ---------------------------------------------------------------
    APP_ENV: Literal["development", "production", "test"] = "development"
    APP_HOST: str = "127.0.0.1"
    APP_PORT: int = 8080
    LOG_LEVEL: str = "INFO"

    # --- Security ----------------------------------------------------------
    SECRET_KEY: str = Field(
        ...,
        min_length=32,
        description="JWT signing key. Generate with secrets.token_urlsafe(64).",
    )
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    BCRYPT_ROUNDS: int = 12

    CORS_ORIGINS: str = "http://localhost:5173,http://localhost:8080"
    ALLOWED_HOSTS: str = "localhost,127.0.0.1"

    # --- Database ----------------------------------------------------------
    DATABASE_URL: str = "sqlite:///./data/neoplast.db"

    # --- Email validation --------------------------------------------------
    EMAIL_API_PROVIDER: Literal["zerobounce", "reoon"] = "zerobounce"
    ZEROBOUNCE_API_KEY: str = ""
    REOON_API_KEY: str = ""

    # --- Web search --------------------------------------------------------
    SERPER_API_KEY: str = ""

    # --- LLM ---------------------------------------------------------------
    OPENAI_API_KEY: str = ""
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_BATCH_SIZE: int = 10

    # --- Pipeline ----------------------------------------------------------
    STALE_THRESHOLD_DAYS: int = 30
    UPLOAD_MAX_SIZE_MB: int = 50
    INGEST_BATCH_SIZE: int = 100

    # --- Website scraper (Phase 4) -----------------------------------------
    # Direct scrape of the prospect's own website yields higher-quality data
    # than Serper snippets. Disable by env if outbound traffic is restricted.
    WEBSITE_SCRAPE_ENABLED: bool = True
    WEBSITE_SCRAPE_TIMEOUT_SECONDS: float = 10.0
    WEBSITE_SCRAPE_MAX_PAGES: int = 2          # homepage + 1 about-page
    WEBSITE_SCRAPE_MAX_BYTES: int = 1_000_000  # 1 MB per page cap
    WEBSITE_SCRAPE_MAX_TEXT_CHARS: int = 50_000  # post-extraction text cap
    WEBSITE_SCRAPE_USER_AGENT: str = (
        "NeoplastBot/1.0 (+https://neoplast.example/bot)"
    )
    WEBSITE_SCRAPE_CONCURRENCY: int = 5

    # --- Rate limiting -----------------------------------------------------
    RATE_LIMIT_LOGIN: str = "5/minute"

    # --- Computed properties ----------------------------------------------
    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def allowed_hosts_list(self) -> list[str]:
        return [h.strip() for h in self.ALLOWED_HOSTS.split(",") if h.strip()]

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def upload_max_bytes(self) -> int:
        return self.UPLOAD_MAX_SIZE_MB * 1024 * 1024

    @field_validator("SECRET_KEY")
    @classmethod
    def secret_key_not_default(cls, v: str) -> str:
        if v.startswith("CHANGE_ME") or v == "secret":
            raise ValueError(
                "SECRET_KEY is set to a placeholder. Generate a real one "
                "with: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
            )
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings accessor (single instance per process)."""
    return Settings()


def ensure_runtime_dirs() -> None:
    """Create runtime data directories if missing."""
    for directory in (DATA_DIR, UPLOADS_DIR, EXPORTS_DIR, BACKUPS_DIR, LOGS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
