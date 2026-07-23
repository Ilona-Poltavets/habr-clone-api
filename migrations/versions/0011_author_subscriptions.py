"""Create author subscriptions.

Revision ID: 0011_author_subscriptions
Revises: 0010_localization_translation_status
Create Date: 2026-07-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011_author_subscriptions"
down_revision: str | Sequence[str] | None = "0010_localization_translation_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "author_subscriptions",
        sa.Column("subscriber_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("author_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["author_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subscriber_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("subscriber_id", "author_id"),
    )
    op.create_index("ix_author_subscriptions_author_id", "author_subscriptions", ["author_id"])


def downgrade() -> None:
    op.drop_table("author_subscriptions")
