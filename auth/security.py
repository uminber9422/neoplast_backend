"""Password hashing + JWT creation/verification primitives.

bcrypt for passwords (cost factor from settings, default 12).
HS256 JWTs signed with SECRET_KEY from environment.
"""

from __future__ import annotations

import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext

from backend.config import get_settings

ALGORITHM = "HS256"

_pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__rounds=get_settings().BCRYPT_ROUNDS,
)


def hash_password(plain: str) -> str:
    """Hash a plaintext password with bcrypt."""
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Constant-time check of a plaintext password against a stored hash."""
    try:
        return _pwd_context.verify(plain, hashed)
    except ValueError:
        # Malformed hash — treat as no match.
        return False


def _create_token(
    *,
    subject: str,
    role: str,
    expires_delta: timedelta,
    token_type: str,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "type": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
        "jti": uuid.uuid4().hex,
    }
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(*, subject: str, role: str) -> str:
    settings = get_settings()
    return _create_token(
        subject=subject,
        role=role,
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )


def create_refresh_token(*, subject: str, role: str) -> str:
    settings = get_settings()
    return _create_token(
        subject=subject,
        role=role,
        expires_delta=timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        token_type="refresh",
    )


def decode_token(token: str, *, expected_type: str) -> dict[str, Any]:
    """Decode a JWT and assert its type. Raises JWTError on any failure."""
    settings = get_settings()
    payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
    if payload.get("type") != expected_type:
        raise JWTError(f"Expected token type {expected_type!r}, got {payload.get('type')!r}")
    return payload


def constant_time_equals(a: str, b: str) -> bool:
    """Constant-time string comparison."""
    return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
