"""Create publication revision history.

Revision ID: 0014_publication_revisions
Revises: 0013_publication_comments
Create Date: 2026-07-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014_publication_revisions"
down_revision: str | Sequence[str] | None = "0013_publication_comments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "publication_revisions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("author_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("category_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("summary", sa.String(length=500), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["author_id"], ["users.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["category_id"], ["categories.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["publication_id"], ["publications.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("publication_id", "revision_number"),
    )
    op.create_index(
        "ix_publication_revisions_publication_id", "publication_revisions", ["publication_id"]
    )
    op.create_index("ix_publication_revisions_author_id", "publication_revisions", ["author_id"])
    op.execute(
        """
        CREATE FUNCTION prevent_publication_revision_changes()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'Publication revisions are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER publication_revisions_immutable
        BEFORE UPDATE OR DELETE ON publication_revisions
        FOR EACH ROW EXECUTE FUNCTION prevent_publication_revision_changes();
        """
    )
    op.execute(
        """
        INSERT INTO publication_revisions (
            id, publication_id, author_id, category_id, revision_number,
            title, summary, body, created_at
        )
        SELECT
            md5(pl.publication_id::text || ':revision:1')::uuid,
            p.id, p.author_id, p.category_id, 1,
            pl.title, pl.summary, pl.body, pl.created_at
        FROM publications p
        JOIN publication_localizations pl
          ON pl.publication_id = p.id AND pl.locale = p.source_locale
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER publication_revisions_immutable ON publication_revisions")
    op.execute("DROP FUNCTION prevent_publication_revision_changes")
    op.drop_table("publication_revisions")
