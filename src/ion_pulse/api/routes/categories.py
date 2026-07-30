from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import User
from ion_pulse.models.publications import Category
from ion_pulse.schemas.categories import CategoryManagementRead, CategoryRead, CategoryUpdate

router = APIRouter(prefix="/categories")


def require_category_management_access(user: User) -> None:
    allowed_roles = {RoleCode.CONTENT_MANAGER.value, RoleCode.ADMINISTRATOR.value}
    if not allowed_roles.intersection(role.code for role in user.roles):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Category management role required"
        )


def category_management_read(category: Category) -> CategoryManagementRead:
    return CategoryManagementRead(
        slug=category.slug,
        name_ru=category.name_ru,
        name_en=category.name_en,
        description_ru=category.description_ru,
        description_en=category.description_en,
        color=category.color,
        sort_order=category.sort_order,
        is_visible=category.is_visible,
    )


@router.get("", response_model=list[CategoryRead])
async def list_visible_categories(
    session: Annotated[AsyncSession, Depends(get_db_session)], locale: str = "ru"
) -> list[CategoryRead]:
    if locale not in {"ru", "en"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported locale"
        )
    categories = (
        await session.scalars(
            select(Category)
            .where(Category.is_visible.is_(True))
            .order_by(Category.sort_order, Category.slug)
        )
    ).all()
    return [
        CategoryRead(
            slug=category.slug,
            name=category.name_ru if locale == "ru" else category.name_en,
            description=category.description_ru if locale == "ru" else category.description_en,
            color=category.color,
            sort_order=category.sort_order,
        )
        for category in categories
    ]


@router.get("/manage", response_model=list[CategoryManagementRead])
async def list_manageable_categories(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> list[CategoryManagementRead]:
    require_category_management_access(user)
    categories = (
        await session.scalars(select(Category).order_by(Category.sort_order, Category.slug))
    ).all()
    return [category_management_read(category) for category in categories]


@router.patch("/{slug}", response_model=CategoryManagementRead)
async def update_category(
    slug: str,
    payload: CategoryUpdate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> CategoryManagementRead:
    require_category_management_access(user)
    category = await session.scalar(select(Category).where(Category.slug == slug))
    if category is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    for field, value in payload.model_dump().items():
        setattr(category, field, value)
    await session.commit()
    await session.refresh(category)
    return category_management_read(category)
