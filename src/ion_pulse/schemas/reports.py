from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ReportCreate(BaseModel):
    reason: str = Field(min_length=10, max_length=1000)


class ReportReview(BaseModel):
    status: str = Field(pattern="^(resolved|dismissed)$")
    review_note: str = Field(min_length=1, max_length=1000)


class ContentReportRead(BaseModel):
    id: UUID
    target_type: str
    target_id: UUID
    reason: str
    status: str
    review_note: str | None
    created_at: datetime
    reviewed_at: datetime | None
    target_author_id: UUID | None = None
    target_excerpt: str | None = None
