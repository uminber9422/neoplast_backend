"""Settings + user-management schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TaxonomyUpdateRequest(BaseModel):
    categories: list[str] = Field(..., min_length=1, max_length=100)


class TaxonomyResponse(BaseModel):
    categories: list[str]


class APIKeyStatus(BaseModel):
    """Boolean flags only — keys themselves are never returned."""

    zerobounce_configured: bool
    serper_configured: bool
    openai_configured: bool


class SettingsOverview(BaseModel):
    taxonomy: list[str]
    api_keys: APIKeyStatus
    stale_threshold_days: int


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(..., min_length=8, max_length=128)
    role: Literal["admin", "sales"]


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    username: str
    role: str
    created_at: datetime
    last_login: datetime | None
