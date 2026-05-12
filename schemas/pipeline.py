"""Pipeline-related schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class PipelineRunRequest(BaseModel):
    run_type: Literal["full", "incremental"] = "incremental"


class PipelineRunSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_type: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    total_records: int
    emails_validated: int
    emails_skipped: int
    prospects_enriched: int
    prospects_skipped: int
    errors: int
    triggered_by: str | None
    log_file: str | None = None


class PipelineRunStatus(PipelineRunSummary):
    current_step: str | None
    progress: dict[str, Any] | None
    error_log: list[dict] | None


class PipelineLogEntry(BaseModel):
    """Single entry from a pipeline run log file.

    The JSONL writer (RunLogger) emits arbitrary fields per event type, so
    we keep the schema permissive and let the response carry raw dicts.
    """

    ts: str | None = None
    run_id: int | None = None
    event: str | None = None
    stage: str | None = None
    email: str | None = None
    data: dict[str, Any] = {}


class PipelineLogResponse(BaseModel):
    """Response for the pipeline logs endpoint."""

    run_id: int
    log_file: str | None
    total_entries: int
    entries: list[dict[str, Any]]
