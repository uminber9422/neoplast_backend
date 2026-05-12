"""Prospect list / detail / filters / export endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from backend.auth.dependencies import get_current_user
from backend.models.database import get_db
from backend.models.prospect import Prospect
from backend.schemas.auth import CurrentUser
from backend.schemas.prospect import (
    ExportFormat,
    ProspectDetail,
    ProspectFilters,
    ProspectListItem,
    ProspectListResponse,
    SortField,
    SortOrder,
)
from backend.utils.export import EXPORT_COLUMNS, export_csv, export_xlsx

router = APIRouter(prefix="/api/prospects", tags=["prospects"])

SORT_COLUMN_MAP = {
    "name": Prospect.name,
    "email": Prospect.email,
    "company_name": Prospect.company_name,
    "industry": Prospect.industry,
    "state": Prospect.state,
    "email_status": Prospect.email_status,
    "relevance_score": Prospect.relevance_score,
    "created_at": Prospect.created_at,
}


def _apply_filters(
    query,
    *,
    search: str | None,
    industry: str | None,
    state: str | None,
    city: str | None,
    country: str | None,
    email_status: str | None,
    source_file: str | None,
):
    if search:
        like = f"%{search.strip()}%"
        query = query.filter(
            or_(
                Prospect.name.ilike(like),
                Prospect.email.ilike(like),
                Prospect.company_name.ilike(like),
            )
        )
    if industry:
        query = query.filter(Prospect.industry == industry)
    if state:
        query = query.filter(Prospect.state == state)
    if city:
        query = query.filter(Prospect.city == city)
    if country:
        query = query.filter(Prospect.country == country)
    if email_status:
        query = query.filter(Prospect.email_status == email_status)
    if source_file:
        query = query.filter(Prospect.source_file == source_file)
    return query


@router.get("", response_model=ProspectListResponse)
@router.get("/", response_model=ProspectListResponse, include_in_schema=False)
def list_prospects(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
    sort: SortField = "created_at",
    order: SortOrder = "desc",
    search: str | None = Query(None, max_length=200),
    industry: str | None = Query(None, max_length=128),
    state: str | None = Query(None, max_length=128),
    city: str | None = Query(None, max_length=128),
    country: str | None = Query(None, max_length=128),
    email_status: str | None = Query(None, max_length=32),
    source_file: str | None = Query(None, max_length=255),
) -> ProspectListResponse:
    query = db.query(Prospect)
    query = _apply_filters(
        query,
        search=search,
        industry=industry,
        state=state,
        city=city,
        country=country,
        email_status=email_status,
        source_file=source_file,
    )

    total = query.with_entities(func.count(Prospect.id)).scalar() or 0
    column = SORT_COLUMN_MAP[sort]
    column = column.asc() if order == "asc" else column.desc()
    rows = query.order_by(column).offset((page - 1) * limit).limit(limit).all()

    return ProspectListResponse(
        items=[ProspectListItem.model_validate(r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


@router.get("/filters", response_model=ProspectFilters)
def filter_options(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> ProspectFilters:
    def _distinct(column) -> list[str]:
        rows = (
            db.query(column)
            .filter(column.isnot(None))
            .distinct()
            .order_by(column.asc())
            .all()
        )
        return [str(r[0]) for r in rows if r[0]]

    return ProspectFilters(
        industries=_distinct(Prospect.industry),
        states=_distinct(Prospect.state),
        cities=_distinct(Prospect.city),
        countries=_distinct(Prospect.country),
        source_files=_distinct(Prospect.source_file),
        email_statuses=_distinct(Prospect.email_status),
    )


@router.get("/export")
def export_prospects(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
    fmt: ExportFormat = Query("csv", alias="format"),
    search: str | None = Query(None, max_length=200),
    industry: str | None = None,
    state: str | None = None,
    city: str | None = None,
    country: str | None = None,
    email_status: str | None = None,
    source_file: str | None = None,
) -> Response:
    query = db.query(Prospect)
    query = _apply_filters(
        query,
        search=search,
        industry=industry,
        state=state,
        city=city,
        country=country,
        email_status=email_status,
        source_file=source_file,
    )
    rows = query.order_by(Prospect.created_at.desc()).all()
    payload = [
        {col: getattr(r, col, None) for col in EXPORT_COLUMNS} for r in rows
    ]

    if fmt == "csv":
        body = export_csv(payload)
        return Response(
            content=body,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="prospects.csv"'},
        )
    body = export_xlsx(payload)
    return Response(
        content=body,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="prospects.xlsx"'},
    )


@router.get("/{prospect_id}", response_model=ProspectDetail)
def get_prospect(
    prospect_id: int,
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> ProspectDetail:
    row = db.query(Prospect).filter(Prospect.id == prospect_id).one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prospect not found")
    return ProspectDetail.model_validate(row)
