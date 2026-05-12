"""Add log_file to pipeline_runs and fix email_activity data.

Revision ID: 0003_add_log_file_fix_activity
Revises: 0002_add_geo_and_csv_extras
Create Date: 2026-04-28

Originally drafted as 0002 in the main checkout's working tree. Renumbered to
0003 during the Phase 1-7 + Pipeline-Logs merge so it chains cleanly after
0002_add_geo_and_csv_extras (the geo / CSV-extras migration shipped with the
international-pipeline overhaul).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_add_log_file_fix_activity"
down_revision: str | None = "0002_add_geo_and_csv_extras"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- 1. Add log_file column to pipeline_runs -------------------------
    with op.batch_alter_table("pipeline_runs") as batch_op:
        batch_op.add_column(sa.Column("log_file", sa.String(length=512), nullable=True))

    # --- 2. Fix email_activity for already-processed records -------------
    # Invalid/abuse emails were incorrectly stored as "active" due to a bug
    # in the email_validator.py `did_you_mean` heuristic.
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "UPDATE prospects SET email_activity = 'inactive' "
            "WHERE email_status IN ('invalid') AND email_activity = 'active'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE prospects SET email_activity = 'unknown' "
            "WHERE email_status IN ('unknown', 'catch-all') AND email_activity = 'active'"
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("pipeline_runs") as batch_op:
        batch_op.drop_column("log_file")
    # Note: we don't reverse the data fix — it was correcting bad data.
