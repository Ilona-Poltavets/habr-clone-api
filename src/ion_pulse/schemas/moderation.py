from datetime import datetime
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field


class SuspensionCreate(BaseModel):
    reason: str = Field(min_length=1, max_length=1000)
    expires_at: AwareDatetime


class SuspensionLift(BaseModel):
    reason: str = Field(min_length=1, max_length=1000)


class UserSanctionRead(BaseModel):
    id: UUID
    user_id: UUID
    action: str
    reason: str
    expires_at: datetime | None
    created_at: datetime


class SanctionAppealCreate(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=128)
    reason: str = Field(min_length=10, max_length=2000)


class SanctionAppealDecision(BaseModel):
    status: str = Field(pattern="^(approved|rejected)$")
    review_note: str = Field(min_length=1, max_length=1000)


class SanctionAppealRead(BaseModel):
    id: UUID
    sanction_id: UUID
    user_id: UUID
    reason: str
    status: str
    review_note: str | None
    reviewed_at: datetime | None
    created_at: datetime
