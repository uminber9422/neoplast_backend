"""Database engine, session factory, and Base declarative class.

Uses SQLAlchemy 2.0 sync API with SQLite. WAL mode enables concurrent
reads while the pipeline writes (PRD §9 risk mitigation). Foreign keys
are enforced via PRAGMA on every connection.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy import create_engine

from backend.config import get_settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


def _build_engine() -> Engine:
    settings = get_settings()
    connect_args: dict[str, Any] = {}
    if settings.DATABASE_URL.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args=connect_args,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )
    return engine


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, _connection_record):  # noqa: ANN001
    """Enable WAL mode + FK enforcement + busy timeout on every connection."""
    if not get_settings().DATABASE_URL.startswith("sqlite"):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=5000")  # ms
    cursor.close()


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency yielding a request-scoped DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create all tables (used in tests / first-run scripts).

    Production schema management goes through Alembic migrations.
    """
    # Import models to register them with Base metadata.
    from backend import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
