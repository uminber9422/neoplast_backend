"""Upload history ORM model — tracks every file ingest."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from backend.models.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UploadHistory(Base):
    __tablename__ = "upload_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    total_records: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    new_records: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    duplicate_records: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_records: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    uploaded_by: Mapped[str | None] = mapped_column(String(64))

    def __repr__(self) -> str:
        return f"<UploadHistory id={self.id} filename={self.filename!r}>"
