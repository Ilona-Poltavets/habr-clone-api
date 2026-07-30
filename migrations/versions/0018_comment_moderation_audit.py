"""Audit comment moderation actions.

Revision ID: 0018_comment_moderation_audit
Revises: 0017_content_reports
Create Date: 2026-07-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0018_comment_moderation_audit"
down_revision: str | Sequence[str] | None = "0017_content_reports"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "comment_moderation_actions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("comment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("moderator_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(length=20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["comment_id"], ["publication_comments.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["moderator_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_comment_moderation_actions_comment_id", "comment_moderation_actions", ["comment_id"]
    )
    op.create_index(
        "ix_comment_moderation_actions_moderator_id", "comment_moderation_actions", ["moderator_id"]
    )
    op.execute(
        """
        CREATE FUNCTION prevent_comment_moderation_action_changes()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'Comment moderation actions are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER comment_moderation_actions_immutable
        BEFORE UPDATE OR DELETE ON comment_moderation_actions
        FOR EACH ROW EXECUTE FUNCTION prevent_comment_moderation_action_changes();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER comment_moderation_actions_immutable ON comment_moderation_actions")
    op.execute("DROP FUNCTION prevent_comment_moderation_action_changes")
    op.drop_table("comment_moderation_actions")
