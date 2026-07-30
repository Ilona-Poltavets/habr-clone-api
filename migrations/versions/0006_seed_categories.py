"""Seed initial categories.

Revision ID: 0006_seed_categories
Revises: 0005_publication_foundation
Create Date: 2026-07-23
"""

# ruff: noqa: E501
from collections.abc import Sequence
from uuid import UUID

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_seed_categories"
down_revision: str | Sequence[str] | None = "0005_publication_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    categories = sa.table(
        "categories",
        sa.column("id", postgresql.UUID(as_uuid=True)),
        sa.column("slug", sa.String),
    )
    op.bulk_insert(
        categories,
        [
            {"id": UUID("b8233204-d411-4b02-ae08-48ecbd3f37e9"), "slug": "reviews"},
            {"id": UUID("5876bf7e-915c-417f-b2bb-b0f2c6655b9b"), "slug": "news"},
            {"id": UUID("be2d7720-e8a2-42c8-8203-31eac75b3d9f"), "slug": "guides"},
            {"id": UUID("45e66025-9a72-4484-b35f-e9f15a70f957"), "slug": "esports"},
        ],
    )


def downgrade() -> None:
    op.execute(
        sa.text("DELETE FROM categories WHERE slug IN ('reviews', 'news', 'guides', 'esports')")
    )
