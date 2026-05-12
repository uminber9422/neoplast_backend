"""Dashboard / overview endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.auth.dependencies import get_current_user
from backend.models.database import get_db
from backend.models.pipeline import PipelineRun
from backend.models.prospect import Prospect
from backend.schemas.auth import CurrentUser
from backend.schemas.dashboard import CountByLabel, DashboardStats

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=DashboardStats)
def stats(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> DashboardStats:
    total = db.query(func.count(Prospect.id)).scalar() or 0

    def _count_where(predicate) -> int:
        return db.query(func.count(Prospect.id)).filter(predicate).scalar() or 0

    valid = _count_where(Prospect.email_status == "valid")
    invalid = _count_where(Prospect.email_status == "invalid")
    risky = _count_where(Prospect.email_status.in_(["catch-all", "disposable"]))
    unknown = _count_where(
        (Prospect.email_status.is_(None)) | (Prospect.email_status == "unknown")
    )

    industry_rows = (
        db.query(Prospect.industry, func.count(Prospect.id))
        .filter(Prospect.industry.isnot(None))
        .group_by(Prospect.industry)
        .order_by(func.count(Prospect.id).desc())
        .limit(10)
        .all()
    )
    state_rows = (
        db.query(Prospect.state, func.count(Prospect.id))
        .filter(Prospect.state.isnot(None))
        .group_by(Prospect.state)
        .order_by(func.count(Prospect.id).desc())
        .limit(10)
        .all()
    )
    status_rows = (
        db.query(Prospect.email_status, func.count(Prospect.id))
        .group_by(Prospect.email_status)
        .all()
    )

    last_run = (
        db.query(PipelineRun)
        .order_by(PipelineRun.id.desc())
        .first()
    )

    return DashboardStats(
        total=total,
        valid=valid,
        invalid=invalid,
        risky=risky,
        unknown=unknown,
        by_industry=[CountByLabel(label=str(i), count=int(c)) for i, c in industry_rows],
        by_state=[CountByLabel(label=str(s), count=int(c)) for s, c in state_rows],
        by_email_status=[
            CountByLabel(label=str(s) if s else "unknown", count=int(c))
            for s, c in status_rows
        ],
        last_run_at=last_run.completed_at or last_run.started_at if last_run else None,
        last_run_status=last_run.status if last_run else None,
    )
