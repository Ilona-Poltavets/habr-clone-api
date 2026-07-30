"""Add publication content types and review metadata.

Revision ID: 0022_publication_content_types
Revises: 0021_localized_categories
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0022_publication_content_types"
down_revision: str | Sequence[str] | None = "0021_localized_categories"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for table_name in ("publications", "publication_revisions"):
        op.add_column(
            table_name,
            sa.Column(
                "content_type", sa.String(length=20), server_default="article", nullable=False
            ),
        )
        op.add_column(
            table_name, sa.Column("game_id", postgresql.UUID(as_uuid=True), nullable=True)
        )
        op.add_column(table_name, sa.Column("review_score", sa.Float(), nullable=True))
        op.create_foreign_key(
            f"fk_{table_name}_game_id",
            table_name,
            "games",
            ["game_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    for table_name in ("publication_revisions", "publications"):
        op.drop_constraint(f"fk_{table_name}_game_id", table_name, type_="foreignkey")
        op.drop_column(table_name, "review_score")
        op.drop_column(table_name, "game_id")
        op.drop_column(table_name, "content_type")
