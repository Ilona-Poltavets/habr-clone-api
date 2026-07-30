"""Idempotent baseline data seeding.

Run after migrations with ``uv run python -m ion_pulse.seeds``. An administrator
is created only when both bootstrap email and password are configured.
"""

import asyncio

from sqlalchemy import select

from ion_pulse.core.config import get_settings
from ion_pulse.core.security import hash_password
from ion_pulse.db.session import async_session_factory, engine
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import Role, User, UserRole
from ion_pulse.models.publications import Category

BASE_CATEGORIES = (
    (
        "reviews", "Game reviews", "Ревью игр", "In-depth impressions and game analysis",
        "Подробные впечатления и разборы игр", "#C7FF5E", 1,
    ),
    (
        "news", "Gaming news", "Игровые новости", "Games industry events and announcements",
        "События и анонсы игровой индустрии", "#A68BFF", 2,
    ),
    (
        "guides", "Guides", "Гайды", "Practical advice for players",
        "Практические советы для игроков", "#72DDF7", 3,
    ),
    (
        "esports", "Esports", "Киберспорт", "Tournaments, teams and competitive gaming",
        "Турниры, команды и соревновательная сцена", "#FFB86B", 4,
    ),
)


async def seed() -> None:
    settings = get_settings()
    async with async_session_factory() as session:
        roles = {role.code: role for role in (await session.scalars(select(Role))).all()}
        for code in RoleCode:
            if code.value not in roles:
                role = Role(code=code.value, name=code.value.replace("_", " ").title())
                session.add(role)
                roles[code.value] = role
        for slug, name_en, name_ru, desc_en, desc_ru, color, order in BASE_CATEGORIES:
            category = await session.scalar(select(Category).where(Category.slug == slug))
            if category is None:
                session.add(
                    Category(
                        slug=slug, name_en=name_en, name_ru=name_ru, description_en=desc_en,
                        description_ru=desc_ru, color=color, sort_order=order,
                    )
                )
        if settings.bootstrap_admin_email and settings.bootstrap_admin_password:
            email = settings.bootstrap_admin_email.lower()
            user = await session.scalar(select(User).where(User.email == email))
            if user is None:
                user = User(
                    email=email,
                    display_name=settings.bootstrap_admin_display_name,
                    password_hash=hash_password(settings.bootstrap_admin_password),
                )
                session.add(user)
                await session.flush()
            elif settings.bootstrap_admin_reset_password:
                user.password_hash = hash_password(settings.bootstrap_admin_password)
            await session.flush()
            administrator = roles[RoleCode.ADMINISTRATOR.value]
            existing_role = await session.get(
                UserRole, {"user_id": user.id, "role_id": administrator.id}
            )
            if existing_role is None:
                session.add(UserRole(user_id=user.id, role_id=administrator.id))
        await session.commit()


async def main() -> None:
    await seed()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
