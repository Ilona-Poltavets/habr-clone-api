"""Record publication status transitions in editorial audit entries.

Revision ID: 0009_audit_publication_status_transitions
Revises: 0008_translation_jobs_and_audit_guards
Create Date: 2026-07-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_audit_publication_status_transitions"
down_revision: str | Sequence[str] | None = "0008_translation_jobs_and_audit_guards"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "publication_editorial_reviews",
        sa.Column("from_status", sa.String(length=30), nullable=False, server_default="draft"),
    )
    op.add_column(
        "publication_editorial_reviews",
        sa.Column("to_status", sa.String(length=30), nullable=False, server_default="draft"),
    )
    op.alter_column(
        "publication_editorial_reviews",
        "note",
        existing_type=sa.String(length=1000),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "publication_editorial_reviews",
        "note",
        existing_type=sa.String(length=1000),
        nullable=True,
    )
    op.drop_column("publication_editorial_reviews", "to_status")
    op.drop_column("publication_editorial_reviews", "from_status")
