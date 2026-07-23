"""Track localization translation state.

Revision ID: 0010_localization_translation_status
Revises: 0009_audit_publication_status_transitions
Create Date: 2026-07-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_localization_translation_status"
down_revision: str | Sequence[str] | None = "0009_audit_publication_status_transitions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "publication_localizations",
        sa.Column(
            "translation_status",
            sa.String(length=20),
            nullable=False,
            server_default="ready",
        ),
    )


def downgrade() -> None:
    op.drop_column("publication_localizations", "translation_status")
