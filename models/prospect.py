"""Prospect ORM model — the central entity (one row per lead)."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.models.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Prospect(Base):
    """A single prospect/lead, deduplicated on email."""

    __tablename__ = "prospects"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # --- Raw fields ---------------------------------------------------------
    name: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False, index=True)
    phone: Mapped[str | None] = mapped_column(String(32))
    company_name: Mapped[str | None] = mapped_column(Text, index=True)
    address: Mapped[str | None] = mapped_column(Text)
    city: Mapped[str | None] = mapped_column(String(128), index=True)
    state: Mapped[str | None] = mapped_column(String(128), index=True)
    pincode: Mapped[str | None] = mapped_column(String(16))
    country: Mapped[str | None] = mapped_column(String(128), index=True)
    website_csv: Mapped[str | None] = mapped_column(String(512))
    notes: Mapped[str | None] = mapped_column(Text)
    fax: Mapped[str | None] = mapped_column(String(32))
    source_file: Mapped[str | None] = mapped_column(String(255), index=True)
    raw_data: Mapped[dict | None] = mapped_column(JSON)
    data_quality_score: Mapped[float | None] = mapped_column(Float)

    # --- Email validation ---------------------------------------------------
    email_status: Mapped[str | None] = mapped_column(String(32), index=True)
    email_sub_status: Mapped[str | None] = mapped_column(String(64))
    email_activity: Mapped[str | None] = mapped_column(String(32))
    email_activity_score: Mapped[float | None] = mapped_column(Float)
    email_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # --- Geography (set by data-profiler stage) -----------------------------
    detected_country_code: Mapped[str | None] = mapped_column(String(2), index=True)
    search_locale: Mapped[str | None] = mapped_column(String(8))

    # --- Enrichment ---------------------------------------------------------
    company_website: Mapped[str | None] = mapped_column(String(512))
    company_linkedin: Mapped[str | None] = mapped_column(String(512))
    person_linkedin: Mapped[str | None] = mapped_column(String(512))
    company_description: Mapped[str | None] = mapped_column(Text)
    industry: Mapped[str | None] = mapped_column(String(128), index=True)
    industry_confidence: Mapped[float | None] = mapped_column(Float)
    sub_category: Mapped[str | None] = mapped_column(String(128))
    company_size: Mapped[str | None] = mapped_column(String(32))
    relevance_score: Mapped[float | None] = mapped_column(Float)
    enrichment_raw: Mapped[dict | None] = mapped_column(JSON)
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # --- Metadata -----------------------------------------------------------
    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    duplicate_of: Mapped[str | None] = mapped_column(String(320))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_prospects_industry_state", "industry", "state"),
        Index("ix_prospects_email_status_industry", "email_status", "industry"),
    )

    def __repr__(self) -> str:
        return f"<Prospect id={self.id} email={self.email!r}>"
