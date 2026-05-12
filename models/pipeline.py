"""Pipeline run ORM model — one row per pipeline execution."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from backend.models.database import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_type: Mapped[str] = mapped_column(String(16), nullable=False)  # full | incremental
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)

    # Stats
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    emails_validated: Mapped[int] = mapped_column(Integer, default=0)
    emails_skipped: Mapped[int] = mapped_column(Integer, default=0)
    prospects_enriched: Mapped[int] = mapped_column(Integer, default=0)
    prospects_skipped: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[int] = mapped_column(Integer, default=0)
    error_log: Mapped[list | None] = mapped_column(JSON)

    # Granular progress for the polling endpoint (PRD §5.3)
    progress: Mapped[dict | None] = mapped_column(JSON)
    current_step: Mapped[str | None] = mapped_column(String(32))
    triggered_by: Mapped[str | None] = mapped_column(String(64))

    # Path to the per-run JSONL trace log written by RunLogger
    # (set in 0003_add_log_file_fix_activity migration).
    log_file: Mapped[str | None] = mapped_column(String(512))

    def __repr__(self) -> str:
        return f"<PipelineRun id={self.id} status={self.status}>"
