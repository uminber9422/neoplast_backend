"""Pytest fixtures: isolated SQLite DB per test session, test client, test users."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest

# Set required env vars BEFORE importing backend modules.
os.environ.setdefault("SECRET_KEY", "test-secret-key-which-is-clearly-long-enough-1234")
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///./data/test.db")
os.environ.setdefault("BCRYPT_ROUNDS", "4")  # fast tests
os.environ.setdefault("RATE_LIMIT_LOGIN", "1000/minute")  # don't trip on fixtures

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from backend.auth.security import hash_password  # noqa: E402
from backend.config import get_settings  # noqa: E402
from backend.main import app  # noqa: E402
from backend.models.database import Base, SessionLocal, engine  # noqa: E402
from backend.models.user import User  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _prepare_db() -> Generator[None, None, None]:
    """Create a fresh test DB once per session."""
    db_path = Path("./data/test.db")
    if db_path.exists():
        try:
            db_path.unlink()
        except PermissionError:
            pass
    Base.metadata.create_all(bind=engine)
    yield
    # Drop all data and dispose of any open connection pools.
    Base.metadata.drop_all(bind=engine)
    engine.dispose()
    # Best-effort cleanup; Windows sometimes still holds the file briefly.
    for _ in range(5):
        try:
            if db_path.exists():
                db_path.unlink()
            break
        except PermissionError:
            import time

            time.sleep(0.1)


@pytest.fixture()
def db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def admin_user(db: Session) -> User:
    user = db.query(User).filter(User.username == "admin_test").one_or_none()
    if user is None:
        user = User(
            username="admin_test",
            password_hash=hash_password("test-pw-12345"),
            role="admin",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    return user


@pytest.fixture()
def sales_user(db: Session) -> User:
    user = db.query(User).filter(User.username == "sales_test").one_or_none()
    if user is None:
        user = User(
            username="sales_test",
            password_hash=hash_password("test-pw-12345"),
            role="sales",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    return user


@pytest.fixture()
def admin_token(client: TestClient, admin_user: User) -> str:
    r = client.post(
        "/api/auth/login",
        json={"username": admin_user.username, "password": "test-pw-12345"},
    )
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture()
def sales_token(client: TestClient, sales_user: User) -> str:
    r = client.post(
        "/api/auth/login",
        json={"username": sales_user.username, "password": "test-pw-12345"},
    )
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture()
def auth_headers(admin_token: str) -> dict:
    return {"Authorization": f"Bearer {admin_token}"}
