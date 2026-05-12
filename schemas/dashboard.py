"""Dashboard / clusters schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class CountByLabel(BaseModel):
    label: str
    count: int


class DashboardStats(BaseModel):
    total: int
    valid: int
    invalid: int
    risky: int  # catch-all + disposable
    unknown: int
    by_industry: list[CountByLabel]
    by_state: list[CountByLabel]
    by_email_status: list[CountByLabel]
    last_run_at: datetime | None
    last_run_status: str | None


class ClusterCard(BaseModel):
    industry: str
    count: int
    valid_pct: float
    top_cities: list[str]
    avg_relevance: float | None


class ClusterDetail(BaseModel):
    industry: str
    count: int
    valid_pct: float
    avg_relevance: float | None
    top_cities: list[CountByLabel]
    top_states: list[CountByLabel]
