from uuid import UUID

from pydantic import BaseModel, Field


class AdminUserRead(BaseModel):
    id: UUID
    email: str
    display_name: str
    is_active: bool
    roles: list[str]


class UserRolesUpdate(BaseModel):
    roles: list[str] = Field(default_factory=list, max_length=5)


class UserRoleAuditRead(BaseModel):
    user_id: UUID
    actor_id: UUID
    role_code: str
    action: str
    created_at: str
