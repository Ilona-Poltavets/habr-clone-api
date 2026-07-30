"""Audit administrator role changes.

Revision ID: 0029_user_role_audit
Revises: 0028_journal_material_snapshots
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0029_user_role_audit"
down_revision: str | Sequence[str] | None = "0028_journal_material_snapshots"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "user_role_audit",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role_code", sa.String(length=50), nullable=False),
        sa.Column("action", sa.String(length=20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["actor_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_user_role_audit_user_id", "user_role_audit", ["user_id"])
    op.create_index("ix_user_role_audit_actor_id", "user_role_audit", ["actor_id"])


def downgrade() -> None:
    op.drop_index("ix_user_role_audit_actor_id", table_name="user_role_audit")
    op.drop_index("ix_user_role_audit_user_id", table_name="user_role_audit")
    op.drop_table("user_role_audit")
