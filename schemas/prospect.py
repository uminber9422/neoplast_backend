"""Prospect-related schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class ProspectListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str | None
    email: str
    phone: str | None
    company_name: str | None
    city: str | None
    state: str | None
    country: str | None
    detected_country_code: str | None
    industry: str | None
    industry_confidence: float | None
    email_status: str | None
    email_activity: str | None
    relevance_score: float | None
    source_file: str | None
    created_at: datetime


class ProspectDetail(ProspectListItem):
    address: str | None
    pincode: str | None
    # CSV-supplied raw fields (Phase 1)
    website_csv: str | None
    notes: str | None
    fax: str | None
    # Geo profiling (Phase 2)
    search_locale: str | None
    # Enrichment outputs
    company_website: str | None
    company_linkedin: str | None
    person_linkedin: str | None
    company_description: str | None
    sub_category: str | None
    company_size: str | None
    email_sub_status: str | None
    email_activity_score: float | None
    email_validated_at: datetime | None
    enriched_at: datetime | None
    data_quality_score: float | None
    raw_data: dict | None
    enrichment_raw: dict | None
    is_duplicate: bool
    duplicate_of: str | None
    updated_at: datetime


class ProspectListResponse(BaseModel):
    items: list[ProspectListItem]
    total: int
    page: int
    limit: int


class ProspectFilters(BaseModel):
    industries: list[str]
    states: list[str]
    cities: list[str]
    countries: list[str]
    source_files: list[str]
    email_statuses: list[str]


SortField = Literal[
    "name",
    "email",
    "company_name",
    "industry",
    "state",
    "email_status",
    "relevance_score",
    "created_at",
]
SortOrder = Literal["asc", "desc"]
ExportFormat = Literal["csv", "xlsx"]
