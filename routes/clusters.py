"""Industry cluster endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.auth.dependencies import get_current_user
from backend.models.database import get_db
from backend.models.prospect import Prospect
from backend.schemas.auth import CurrentUser
from backend.schemas.dashboard import ClusterCard, ClusterDetail, CountByLabel

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


@router.get("", response_model=list[ClusterCard])
@router.get("/", response_model=list[ClusterCard], include_in_schema=False)
def list_clusters(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> list[ClusterCard]:
    industries = (
        db.query(
            Prospect.industry,
            func.count(Prospect.id),
            func.avg(Prospect.relevance_score),
        )
        .filter(Prospect.industry.isnot(None))
        .group_by(Prospect.industry)
        .order_by(func.count(Prospect.id).desc())
        .all()
    )
    out: list[ClusterCard] = []
    for industry, count, avg_rel in industries:
        valid_count = (
            db.query(func.count(Prospect.id))
            .filter(Prospect.industry == industry, Prospect.email_status == "valid")
            .scalar()
            or 0
        )
        cities = (
            db.query(Prospect.city, func.count(Prospect.id))
            .filter(Prospect.industry == industry, Prospect.city.isnot(None))
            .group_by(Prospect.city)
            .order_by(func.count(Prospect.id).desc())
            .limit(3)
            .all()
        )
        out.append(
            ClusterCard(
                industry=str(industry),
                count=int(count),
                valid_pct=round((valid_count / count * 100) if count else 0.0, 1),
                top_cities=[str(c) for c, _ in cities],
                avg_relevance=float(avg_rel) if avg_rel is not None else None,
            )
        )
    return out


@router.get("/{industry}", response_model=ClusterDetail)
def cluster_detail(
    industry: str,
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> ClusterDetail:
    base = db.query(Prospect).filter(Prospect.industry == industry)
    count = base.count()
    valid_count = base.filter(Prospect.email_status == "valid").count()
    avg_rel = (
        db.query(func.avg(Prospect.relevance_score))
        .filter(Prospect.industry == industry)
        .scalar()
    )
    cities = (
        db.query(Prospect.city, func.count(Prospect.id))
        .filter(Prospect.industry == industry, Prospect.city.isnot(None))
        .group_by(Prospect.city)
        .order_by(func.count(Prospect.id).desc())
        .limit(10)
        .all()
    )
    states = (
        db.query(Prospect.state, func.count(Prospect.id))
        .filter(Prospect.industry == industry, Prospect.state.isnot(None))
        .group_by(Prospect.state)
        .order_by(func.count(Prospect.id).desc())
        .limit(10)
        .all()
    )
    return ClusterDetail(
        industry=industry,
        count=count,
        valid_pct=round((valid_count / count * 100) if count else 0.0, 1),
        avg_relevance=float(avg_rel) if avg_rel is not None else None,
        top_cities=[CountByLabel(label=str(c), count=int(n)) for c, n in cities],
        top_states=[CountByLabel(label=str(s), count=int(n)) for s, n in states],
    )
