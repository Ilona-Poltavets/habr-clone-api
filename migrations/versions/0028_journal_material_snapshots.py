"""Snapshot journal materials on publication.

Revision ID: 0028_journal_material_snapshots
Revises: 0027_password_reset_tokens
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028_journal_material_snapshots"
down_revision: str | Sequence[str] | None = "0027_password_reset_tokens"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("journal_issue_publications", sa.Column("snapshot", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("journal_issue_publications", "snapshot")
