"""Create immutable editorial decision audit records.

Revision ID: 0007_publication_editorial_reviews
Revises: 0006_seed_categories
Create Date: 2026-07-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007_publication_editorial_reviews"
down_revision: str | Sequence[str] | None = "0006_seed_categories"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Older Alembic installations create this bookkeeping column as VARCHAR(32),
    # while this and later descriptive revision IDs are longer.
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=64),
    )
    op.create_table(
        "publication_editorial_reviews",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reviewer_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("decision", sa.String(length=30), nullable=False),
        sa.Column("note", sa.String(length=1000)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["publication_id"], ["publications.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["reviewer_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_publication_editorial_reviews_publication_id",
        "publication_editorial_reviews",
        ["publication_id"],
    )
    op.create_index(
        "ix_publication_editorial_reviews_reviewer_id",
        "publication_editorial_reviews",
        ["reviewer_id"],
    )


def downgrade() -> None:
    op.drop_table("publication_editorial_reviews")
