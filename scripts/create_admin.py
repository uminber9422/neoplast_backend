"""Interactive script to create the first admin user.

Run after the database has been migrated:
    python scripts/create_admin.py
"""

from __future__ import annotations

import getpass
import re
import sys
from pathlib import Path

# Make `backend.*` importable when running from project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.auth.security import hash_password  # noqa: E402
from backend.models.database import SessionLocal, init_db  # noqa: E402
from backend.models.user import User  # noqa: E402

USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,64}$")


def _prompt_username() -> str:
    while True:
        username = input("Username (3-64 chars, alphanumeric/_-.): ").strip()
        if USERNAME_RE.match(username):
            return username
        print("  ✗ Invalid username. Try again.")


def _prompt_password() -> str:
    while True:
        pw = getpass.getpass("Password (min 8 chars): ")
        if len(pw) < 8:
            print("  ✗ Password must be at least 8 characters.")
            continue
        confirm = getpass.getpass("Confirm password: ")
        if pw != confirm:
            print("  ✗ Passwords don't match.")
            continue
        return pw


def main() -> int:
    print("=== Neoplast Lead Dashboard — admin user setup ===\n")
    init_db()  # idempotent — safe even if Alembic already ran.
    db = SessionLocal()
    try:
        existing_admin_count = db.query(User).filter(User.role == "admin").count()
        if existing_admin_count > 0:
            print(f"  ! {existing_admin_count} admin(s) already exist.")
            cont = input("Create another admin? (y/N): ").strip().lower()
            if cont != "y":
                return 0

        username = _prompt_username()
        if db.query(User).filter(User.username == username).first():
            print(f"  ✗ User {username!r} already exists.")
            return 1

        password = _prompt_password()
        user = User(
            username=username,
            password_hash=hash_password(password),
            role="admin",
        )
        db.add(user)
        db.commit()
        print(f"\n  ✓ Admin user {username!r} created.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
