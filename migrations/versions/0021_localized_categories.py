"""Add localized category metadata.

Revision ID: 0021_localized_categories
Revises: 0020_publication_ai_reviews
"""
# ruff: noqa: E501

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0021_localized_categories"
down_revision: str | Sequence[str] | None = "0020_publication_ai_reviews"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for _name, column in (
        ("name_ru", sa.Column("name_ru", sa.String(length=120), server_default="", nullable=False)),
        ("name_en", sa.Column("name_en", sa.String(length=120), server_default="", nullable=False)),
        (
            "description_ru",
            sa.Column("description_ru", sa.String(length=500), server_default="", nullable=False),
        ),
        (
            "description_en",
            sa.Column("description_en", sa.String(length=500), server_default="", nullable=False),
        ),
        (
            "color",
            sa.Column("color", sa.String(length=7), server_default="#C7FF5E", nullable=False),
        ),
        ("sort_order", sa.Column("sort_order", sa.Integer(), server_default="0", nullable=False)),
        (
            "is_visible",
            sa.Column("is_visible", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        ),
    ):
        op.add_column("categories", column)
    op.execute(
        """UPDATE categories SET name_ru = CASE slug WHEN 'reviews' THEN 'Ревью игр' WHEN 'news' THEN 'Игровые новости' WHEN 'guides' THEN 'Гайды' WHEN 'esports' THEN 'Киберспорт' ELSE slug END, name_en = CASE slug WHEN 'reviews' THEN 'Game reviews' WHEN 'news' THEN 'Gaming news' WHEN 'guides' THEN 'Guides' WHEN 'esports' THEN 'Esports' ELSE slug END, description_ru = CASE slug WHEN 'reviews' THEN 'Подробные впечатления и разборы игр' WHEN 'news' THEN 'События и анонсы игровой индустрии' WHEN 'guides' THEN 'Практические советы для игроков' WHEN 'esports' THEN 'Турниры, команды и соревновательная сцена' ELSE '' END, description_en = CASE slug WHEN 'reviews' THEN 'In-depth impressions and game analysis' WHEN 'news' THEN 'Events and announcements from the games industry' WHEN 'guides' THEN 'Practical advice for players' WHEN 'esports' THEN 'Tournaments, teams and competitive gaming' ELSE '' END, sort_order = CASE slug WHEN 'reviews' THEN 1 WHEN 'news' THEN 2 WHEN 'guides' THEN 3 WHEN 'esports' THEN 4 ELSE 99 END"""
    )


def downgrade() -> None:
    for name in (
        "is_visible",
        "sort_order",
        "color",
        "description_en",
        "description_ru",
        "name_en",
        "name_ru",
    ):
        op.drop_column("categories", name)
