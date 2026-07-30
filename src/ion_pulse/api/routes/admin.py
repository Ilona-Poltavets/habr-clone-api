from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ion_pulse.api.routes.auth import get_current_user
from ion_pulse.db.session import get_db_session
from ion_pulse.domain.roles import RoleCode
from ion_pulse.models.identity import Role, User, UserRole, UserRoleAudit
from ion_pulse.schemas.admin import AdminUserRead, UserRoleAuditRead, UserRolesUpdate

router = APIRouter(prefix="/admin")


def require_administrator(user: User) -> None:
    if RoleCode.ADMINISTRATOR.value not in {role.code for role in user.roles}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Administrator role required"
        )


def to_admin_user(user: User) -> AdminUserRead:
    return AdminUserRead(
        id=user.id,
        email=user.email,
        display_name=user.display_name,
        is_active=user.is_active,
        roles=sorted(role.code for role in user.roles),
    )


@router.get("/users", response_model=list[AdminUserRead])
async def list_users(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    administrator: Annotated[User, Depends(get_current_user)],
) -> list[AdminUserRead]:
    require_administrator(administrator)
    users = (
        await session.scalars(
            select(User).options(selectinload(User.roles)).order_by(User.created_at)
        )
    ).all()
    return [to_admin_user(user) for user in users]


@router.get("/role-audit", response_model=list[UserRoleAuditRead])
async def list_role_audit(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    administrator: Annotated[User, Depends(get_current_user)],
) -> list[UserRoleAuditRead]:
    require_administrator(administrator)
    entries = (
        await session.scalars(
            select(UserRoleAudit).order_by(UserRoleAudit.created_at.desc()).limit(200)
        )
    ).all()
    return [
        UserRoleAuditRead(
            user_id=entry.user_id,
            actor_id=entry.actor_id,
            role_code=entry.role_code,
            action=entry.action,
            created_at=entry.created_at.isoformat(),
        )
        for entry in entries
    ]


@router.put("/users/{user_id}/roles", response_model=AdminUserRead)
async def update_user_roles(
    user_id: UUID,
    payload: UserRolesUpdate,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    administrator: Annotated[User, Depends(get_current_user)],
) -> AdminUserRead:
    require_administrator(administrator)
    requested = set(payload.roles)
    valid = {role.value for role in RoleCode}
    if not requested <= valid:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unknown role")
    user = await session.scalar(
        select(User).options(selectinload(User.roles)).where(User.id == user_id)
    )
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    current = {role.code for role in user.roles}
    if user.id == administrator.id and RoleCode.ADMINISTRATOR.value not in requested:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove your own administrator role",
        )
    roles = (await session.scalars(select(Role))).all()
    by_code = {role.code: role for role in roles}
    for role_code in current - requested:
        role = by_code[role_code]
        user_role = await session.get(UserRole, {"user_id": user.id, "role_id": role.id})
        if user_role is not None:
            await session.delete(user_role)
        session.add(
            UserRoleAudit(
                user_id=user.id,
                actor_id=administrator.id,
                role_code=role_code,
                action="revoked",
            )
        )
    for role_code in requested - current:
        role = by_code[role_code]
        session.add(UserRole(user_id=user.id, role_id=role.id))
        session.add(
            UserRoleAudit(
                user_id=user.id,
                actor_id=administrator.id,
                role_code=role_code,
                action="granted",
            )
        )
    await session.commit()
    refreshed = await session.scalar(
        select(User).options(selectinload(User.roles)).where(User.id == user.id)
    )
    if refreshed is None:
        raise RuntimeError("Updated user is missing")
    return to_admin_user(refreshed)
