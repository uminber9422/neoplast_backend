"""Auth routes — login, token refresh."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from jose import JWTError
from sqlalchemy.orm import Session

from backend.auth.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    verify_password,
)
from backend.config import get_settings
from backend.logging_setup import get_logger
from backend.middleware.rate_limit import make_rate_limit_dep
from backend.models import User
from backend.models.database import get_db
from backend.schemas.auth import (
    AccessTokenResponse,
    LoginRequest,
    RefreshRequest,
    TokenResponse,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])
log = get_logger(__name__)
_login_rate_limit = make_rate_limit_dep(get_settings().RATE_LIMIT_LOGIN)


@router.post("/login", response_model=TokenResponse)
def login(
    payload: LoginRequest,
    db: Session = Depends(get_db),
    _rate_limit: None = Depends(_login_rate_limit),
) -> TokenResponse:
    """Authenticate user and return access + refresh tokens.

    Rate-limited to mitigate brute force. Generic error message avoids
    confirming whether the username exists.
    """
    user = db.query(User).filter(User.username == payload.username).one_or_none()
    if user is None or not verify_password(payload.password, user.password_hash):
        log.info("login_failed", username=payload.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    user.last_login = datetime.now(timezone.utc)
    db.commit()

    log.info("login_success", username=user.username, role=user.role)
    return TokenResponse(
        access_token=create_access_token(subject=user.username, role=user.role),
        refresh_token=create_refresh_token(subject=user.username, role=user.role),
        role=user.role,
        username=user.username,
    )


@router.post("/refresh", response_model=AccessTokenResponse)
def refresh_token(
    payload: RefreshRequest,
    db: Session = Depends(get_db),
) -> AccessTokenResponse:
    """Exchange a valid refresh token for a new access token.

    Verifies the user still exists (handles deleted users).
    """
    try:
        claims = decode_token(payload.refresh_token, expected_type="refresh")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        ) from None

    username = claims.get("sub")
    user = db.query(User).filter(User.username == username).one_or_none()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User no longer exists",
        )

    return AccessTokenResponse(
        access_token=create_access_token(subject=user.username, role=user.role),
    )
