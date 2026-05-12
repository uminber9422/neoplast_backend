"""File upload + history endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from backend.auth.dependencies import require_admin
from backend.config import UPLOADS_DIR, get_settings
from backend.logging_setup import get_logger
from backend.models.database import get_db
from backend.models.upload import UploadHistory
from backend.pipeline.ingest import ingest_file
from backend.schemas.auth import CurrentUser
from backend.schemas.upload import UploadHistoryItem, UploadResponse
from backend.utils.filenames import (
    ALLOWED_EXTENSIONS,
    has_allowed_extension,
    is_within_directory,
    sanitize_filename,
)

router = APIRouter(prefix="/api/uploads", tags=["uploads"])
log = get_logger(__name__)


@router.post("", response_model=UploadResponse)
@router.post("/", response_model=UploadResponse, include_in_schema=False)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_admin),
) -> UploadResponse:
    settings = get_settings()
    original = file.filename or "upload"

    if not has_allowed_extension(original):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {', '.join(sorted(ALLOWED_EXTENSIONS))} files are allowed.",
        )

    safe_name = sanitize_filename(original)
    target = UPLOADS_DIR / safe_name

    if not is_within_directory(target, UPLOADS_DIR):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid filename.")

    # Stream-write with size enforcement to avoid loading large files entirely in memory.
    written = 0
    max_bytes = settings.upload_max_bytes
    try:
        with target.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                if written > max_bytes:
                    out.close()
                    target.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"File exceeds {settings.UPLOAD_MAX_SIZE_MB} MB limit.",
                    )
                out.write(chunk)
    finally:
        await file.close()

    try:
        result = ingest_file(db, target, uploaded_by=user.username)
    except ValueError as exc:
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from None
    except Exception as exc:  # noqa: BLE001
        log.exception("ingest_failed", file=safe_name, error=str(exc))
        target.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ingest failed. Check server logs.",
        ) from None

    upload_id = (
        db.query(UploadHistory.id)
        .filter(UploadHistory.filename == result.filename)
        .order_by(UploadHistory.id.desc())
        .first()
    )
    return UploadResponse(
        upload_id=int(upload_id[0]) if upload_id else 0,
        filename=result.filename,
        total=result.total,
        new=result.new,
        duplicates=result.duplicates,
        skipped=result.skipped,
        errors=result.errors,
    )


@router.get("/history", response_model=list[UploadHistoryItem])
def upload_history(
    db: Session = Depends(get_db),
    _admin: CurrentUser = Depends(require_admin),
) -> list[UploadHistoryItem]:
    rows = db.query(UploadHistory).order_by(UploadHistory.id.desc()).limit(100).all()
    return [UploadHistoryItem.model_validate(r) for r in rows]
