"""Create asynchronous publication AI reviews.

Revision ID: 0020_publication_ai_reviews
Revises: 0019_user_sanctions
Create Date: 2026-07-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0020_publication_ai_reviews"
down_revision: str | Sequence[str] | None = "0019_user_sanctions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "publication_ai_reviews",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="pending", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("decision", sa.String(length=30)),
        sa.Column("risk_categories", postgresql.JSONB()),
        sa.Column("reasons", postgresql.JSONB()),
        sa.Column("confidence", sa.Float()),
        sa.Column("age_rating", sa.Integer()),
        sa.Column("provider", sa.String(length=100)),
        sa.Column("model", sa.String(length=100)),
        sa.Column("rules_version", sa.String(length=100)),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["publication_id"], ["publications.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("publication_id", "source_revision"),
    )
    op.create_index(
        "ix_publication_ai_reviews_publication_id", "publication_ai_reviews", ["publication_id"]
    )


def downgrade() -> None:
    op.drop_table("publication_ai_reviews")
