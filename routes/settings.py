"""Settings (admin only): taxonomy, API key status, user management.

API keys themselves are never returned by these endpoints — only configured/not
status booleans. Keys must be edited in `.env` and the service restarted.
This avoids storing secrets in the DB.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from backend.auth.dependencies import require_admin
from backend.auth.security import hash_password
from backend.config import get_settings
from backend.models.database import get_db
from backend.models.user import User
from backend.schemas.auth import CurrentUser
from backend.schemas.settings import (
    APIKeyStatus,
    SettingsOverview,
    TaxonomyResponse,
    TaxonomyUpdateRequest,
    UserCreateRequest,
    UserResponse,
)
from backend.utils.industry_taxonomy import get_taxonomy, set_taxonomy

router = APIRouter(prefix="/api/settings", tags=["settings"])


@router.get("", response_model=SettingsOverview)
@router.get("/", response_model=SettingsOverview, include_in_schema=False)
def overview(
    db: Session = Depends(get_db),
    _admin: CurrentUser = Depends(require_admin),
) -> SettingsOverview:
    settings = get_settings()
    return SettingsOverview(
        taxonomy=get_taxonomy(db),
        api_keys=APIKeyStatus(
            zerobounce_configured=bool(settings.ZEROBOUNCE_API_KEY),
            serper_configured=bool(settings.SERPER_API_KEY),
            openai_configured=bool(settings.OPENAI_API_KEY),
        ),
        stale_threshold_days=settings.STALE_THRESHOLD_DAYS,
    )


@router.put("/taxonomy", response_model=TaxonomyResponse)
def update_taxonomy(
    payload: TaxonomyUpdateRequest,
    db: Session = Depends(get_db),
    _admin: CurrentUser = Depends(require_admin),
) -> TaxonomyResponse:
    cleaned = set_taxonomy(db, payload.categories)
    return TaxonomyResponse(categories=cleaned)


@router.get("/users", response_model=list[UserResponse])
def list_users(
    db: Session = Depends(get_db),
    _admin: CurrentUser = Depends(require_admin),
) -> list[UserResponse]:
    rows = db.query(User).order_by(User.username.asc()).all()
    return [UserResponse.model_validate(r) for r in rows]


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreateRequest,
    db: Session = Depends(get_db),
    _admin: CurrentUser = Depends(require_admin),
) -> UserResponse:
    if db.query(User).filter(User.username == payload.username).first() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already exists",
        )
    user = User(
        username=payload.username,
        password_hash=hash_password(payload.password),
        role=payload.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return UserResponse.model_validate(user)


@router.delete(
    "/users/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    admin: CurrentUser = Depends(require_admin),
) -> Response:
    user = db.query(User).filter(User.id == user_id).one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if user.username == admin.username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account",
        )
    # Don't let an admin delete the last admin (lockout protection)
    if user.role == "admin":
        remaining_admins = (
            db.query(User)
            .filter(User.role == "admin", User.id != user.id)
            .count()
        )
        if remaining_admins == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete the only remaining admin",
            )
    db.delete(user)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
