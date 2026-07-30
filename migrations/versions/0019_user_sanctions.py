"""Create reversible user sanctions.

Revision ID: 0019_user_sanctions
Revises: 0018_comment_moderation_audit
Create Date: 2026-07-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0019_user_sanctions"
down_revision: str | Sequence[str] | None = "0018_comment_moderation_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("users", sa.Column("suspended_until", sa.DateTime(timezone=True)))
    op.create_table(
        "user_sanctions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("moderator_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(length=20), nullable=False),
        sa.Column("reason", sa.String(length=1000), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["moderator_id"], ["users.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_user_sanctions_user_id", "user_sanctions", ["user_id"])
    op.create_index("ix_user_sanctions_moderator_id", "user_sanctions", ["moderator_id"])
    op.execute(
        """
        CREATE FUNCTION prevent_user_sanction_changes()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'User sanctions are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER user_sanctions_immutable
        BEFORE UPDATE OR DELETE ON user_sanctions
        FOR EACH ROW EXECUTE FUNCTION prevent_user_sanction_changes();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER user_sanctions_immutable ON user_sanctions")
    op.execute("DROP FUNCTION prevent_user_sanction_changes")
    op.drop_table("user_sanctions")
    op.drop_column("users", "suspended_until")
