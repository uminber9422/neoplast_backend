"""Upload-related schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class UploadResponse(BaseModel):
    upload_id: int
    filename: str
    total: int
    new: int
    duplicates: int
    skipped: int
    errors: list[str] = []


class UploadHistoryItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    filename: str
    uploaded_at: datetime
    total_records: int
    new_records: int
    duplicate_records: int
    skipped_records: int
    uploaded_by: str | None
