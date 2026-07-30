"""Add selected publications for editorial digests.

Revision ID: 0023_digest_publications
Revises: 0022_publication_content_types
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0023_digest_publications"
down_revision: str | Sequence[str] | None = "0022_publication_content_types"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "digest_publications",
        sa.Column("digest_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["digest_id"], ["publications.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publication_id"], ["publications.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("digest_id", "publication_id"),
        sa.UniqueConstraint("digest_id", "position"),
    )


def downgrade() -> None:
    op.drop_table("digest_publications")
