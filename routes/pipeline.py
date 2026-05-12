"""Pipeline trigger / status / history / logs endpoints."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from backend.auth.dependencies import get_current_user, require_admin
from backend.models.database import get_db
from backend.models.pipeline import PipelineRun
from backend.pipeline import orchestrator
from backend.pipeline.run_logger import read_run_log
from backend.schemas.auth import CurrentUser
from backend.schemas.pipeline import (
    PipelineLogResponse,
    PipelineRunRequest,
    PipelineRunStatus,
    PipelineRunSummary,
)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.post("/run", response_model=PipelineRunSummary)
async def run(
    payload: PipelineRunRequest,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_admin),
) -> PipelineRunSummary:
    try:
        run_obj = orchestrator.kickoff(
            db,
            run_type=payload.run_type,
            triggered_by=user.username,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from None

    asyncio.create_task(orchestrator.run_pipeline(run_obj.id))
    return PipelineRunSummary.model_validate(run_obj)


@router.get("/status", response_model=PipelineRunStatus | None)
def latest_status(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> PipelineRunStatus | None:
    row = db.query(PipelineRun).order_by(PipelineRun.id.desc()).first()
    if row is None:
        return None
    return PipelineRunStatus.model_validate(row)


@router.get("/status/{run_id}", response_model=PipelineRunStatus)
def status_by_id(
    run_id: int,
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> PipelineRunStatus:
    row = db.query(PipelineRun).filter(PipelineRun.id == run_id).one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return PipelineRunStatus.model_validate(row)


@router.get("/history", response_model=list[PipelineRunSummary])
def history(
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
) -> list[PipelineRunSummary]:
    rows = db.query(PipelineRun).order_by(PipelineRun.id.desc()).limit(50).all()
    return [PipelineRunSummary.model_validate(r) for r in rows]


@router.get("/{run_id}/logs", response_model=PipelineLogResponse)
def get_run_logs(
    run_id: int,
    db: Session = Depends(get_db),
    _user: CurrentUser = Depends(get_current_user),
    stage: str | None = Query(None, description="Filter logs by pipeline stage"),
    email: str | None = Query(None, description="Filter logs by email address"),
) -> PipelineLogResponse:
    """Retrieve the full trace log for a specific pipeline run.

    Supports filtering by ``stage`` (email_validation, data_profiling,
    website_scraping, web_search, llm_extraction) and/or ``email`` address.
    """
    row = db.query(PipelineRun).filter(PipelineRun.id == run_id).one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    entries = read_run_log(run_id, stage=stage, email=email)

    return PipelineLogResponse(
        run_id=run_id,
        log_file=row.log_file,
        total_entries=len(entries),
        entries=entries,
    )
