"""Add geography + CSV-extras columns to prospects.

Adds: country, website_csv, notes, fax (raw CSV fields)
Adds: detected_country_code, search_locale (set later by data-profiler stage)

Revision ID: 0002_add_geo_and_csv_extras
Revises: 0001_initial_schema
Create Date: 2026-04-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_add_geo_and_csv_extras"
down_revision: str | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("prospects") as batch_op:
        batch_op.add_column(sa.Column("country", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("website_csv", sa.String(length=512), nullable=True))
        batch_op.add_column(sa.Column("notes", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("fax", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("detected_country_code", sa.String(length=2), nullable=True)
        )
        batch_op.add_column(sa.Column("search_locale", sa.String(length=8), nullable=True))

    op.create_index("ix_prospects_country", "prospects", ["country"])
    op.create_index(
        "ix_prospects_detected_country_code", "prospects", ["detected_country_code"]
    )


def downgrade() -> None:
    op.drop_index("ix_prospects_detected_country_code", table_name="prospects")
    op.drop_index("ix_prospects_country", table_name="prospects")
    with op.batch_alter_table("prospects") as batch_op:
        batch_op.drop_column("search_locale")
        batch_op.drop_column("detected_country_code")
        batch_op.drop_column("fax")
        batch_op.drop_column("notes")
        batch_op.drop_column("website_csv")
        batch_op.drop_column("country")
