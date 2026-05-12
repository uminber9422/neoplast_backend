"""Initial schema — prospects, uploads, pipeline runs, users, settings.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-04-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "prospects",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("company_name", sa.Text(), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=True),
        sa.Column("state", sa.String(length=128), nullable=True),
        sa.Column("pincode", sa.String(length=16), nullable=True),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.Column("raw_data", sa.JSON(), nullable=True),
        sa.Column("data_quality_score", sa.Float(), nullable=True),
        sa.Column("email_status", sa.String(length=32), nullable=True),
        sa.Column("email_sub_status", sa.String(length=64), nullable=True),
        sa.Column("email_activity", sa.String(length=32), nullable=True),
        sa.Column("email_activity_score", sa.Float(), nullable=True),
        sa.Column("email_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("company_website", sa.String(length=512), nullable=True),
        sa.Column("company_linkedin", sa.String(length=512), nullable=True),
        sa.Column("person_linkedin", sa.String(length=512), nullable=True),
        sa.Column("company_description", sa.Text(), nullable=True),
        sa.Column("industry", sa.String(length=128), nullable=True),
        sa.Column("industry_confidence", sa.Float(), nullable=True),
        sa.Column("sub_category", sa.String(length=128), nullable=True),
        sa.Column("company_size", sa.String(length=32), nullable=True),
        sa.Column("relevance_score", sa.Float(), nullable=True),
        sa.Column("enrichment_raw", sa.JSON(), nullable=True),
        sa.Column("enriched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_duplicate", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("duplicate_of", sa.String(length=320), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("email", name="uq_prospects_email"),
    )
    op.create_index("ix_prospects_email", "prospects", ["email"])
    op.create_index("ix_prospects_company_name", "prospects", ["company_name"])
    op.create_index("ix_prospects_city", "prospects", ["city"])
    op.create_index("ix_prospects_state", "prospects", ["state"])
    op.create_index("ix_prospects_source_file", "prospects", ["source_file"])
    op.create_index("ix_prospects_email_status", "prospects", ["email_status"])
    op.create_index("ix_prospects_industry", "prospects", ["industry"])
    op.create_index("ix_prospects_industry_state", "prospects", ["industry", "state"])
    op.create_index(
        "ix_prospects_email_status_industry", "prospects", ["email_status", "industry"]
    )

    op.create_table(
        "upload_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_records", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_records", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_records", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_records", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("uploaded_by", sa.String(length=64), nullable=True),
    )

    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_type", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column("total_records", sa.Integer(), server_default="0"),
        sa.Column("emails_validated", sa.Integer(), server_default="0"),
        sa.Column("emails_skipped", sa.Integer(), server_default="0"),
        sa.Column("prospects_enriched", sa.Integer(), server_default="0"),
        sa.Column("prospects_skipped", sa.Integer(), server_default="0"),
        sa.Column("errors", sa.Integer(), server_default="0"),
        sa.Column("error_log", sa.JSON(), nullable=True),
        sa.Column("progress", sa.JSON(), nullable=True),
        sa.Column("current_step", sa.String(length=32), nullable=True),
        sa.Column("triggered_by", sa.String(length=64), nullable=True),
    )

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("username", name="uq_users_username"),
    )
    op.create_index("ix_users_username", "users", ["username"])

    op.create_table(
        "settings",
        sa.Column("key", sa.String(length=64), primary_key=True),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("settings")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_table("users")
    op.drop_table("pipeline_runs")
    op.drop_table("upload_history")
    op.drop_index("ix_prospects_email_status_industry", table_name="prospects")
    op.drop_index("ix_prospects_industry_state", table_name="prospects")
    op.drop_index("ix_prospects_industry", table_name="prospects")
    op.drop_index("ix_prospects_email_status", table_name="prospects")
    op.drop_index("ix_prospects_source_file", table_name="prospects")
    op.drop_index("ix_prospects_state", table_name="prospects")
    op.drop_index("ix_prospects_city", table_name="prospects")
    op.drop_index("ix_prospects_company_name", table_name="prospects")
    op.drop_index("ix_prospects_email", table_name="prospects")
    op.drop_table("prospects")
