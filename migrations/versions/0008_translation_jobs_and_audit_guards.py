"""Add translation jobs and protect editorial audit records.

Revision ID: 0008_translation_jobs_and_audit_guards
Revises: 0007_publication_editorial_reviews
Create Date: 2026-07-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008_translation_jobs_and_audit_guards"
down_revision: str | Sequence[str] | None = "0007_publication_editorial_reviews"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "publication_editorial_reviews_publication_id_fkey",
        "publication_editorial_reviews",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "publication_editorial_reviews_publication_id_fkey",
        "publication_editorial_reviews",
        "publications",
        ["publication_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.execute(
        """
        CREATE FUNCTION prevent_publication_editorial_review_changes()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'Publication editorial reviews are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER publication_editorial_reviews_immutable
        BEFORE UPDATE OR DELETE ON publication_editorial_reviews
        FOR EACH ROW EXECUTE FUNCTION prevent_publication_editorial_review_changes();
        """
    )
    op.create_table(
        "translation_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_locale", sa.String(length=5), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="pending", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["publication_id"], ["publications.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("publication_id", "target_locale"),
    )
    op.create_index("ix_translation_jobs_publication_id", "translation_jobs", ["publication_id"])


def downgrade() -> None:
    op.drop_table("translation_jobs")
    op.execute(
        "DROP TRIGGER publication_editorial_reviews_immutable ON publication_editorial_reviews"
    )
    op.execute("DROP FUNCTION prevent_publication_editorial_review_changes")
    op.drop_constraint(
        "publication_editorial_reviews_publication_id_fkey",
        "publication_editorial_reviews",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "publication_editorial_reviews_publication_id_fkey",
        "publication_editorial_reviews",
        "publications",
        ["publication_id"],
        ["id"],
        ondelete="CASCADE",
    )
