"""Add scheduled publication time.

Revision ID: 0015_scheduled_publications
Revises: 0014_publication_revisions
Create Date: 2026-07-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_scheduled_publications"
down_revision: str | Sequence[str] | None = "0014_publication_revisions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("publications", sa.Column("scheduled_at", sa.DateTime(timezone=True)))
    op.create_index("ix_publications_scheduled_at", "publications", ["scheduled_at"])


def downgrade() -> None:
    op.drop_index("ix_publications_scheduled_at", table_name="publications")
    op.drop_column("publications", "scheduled_at")
