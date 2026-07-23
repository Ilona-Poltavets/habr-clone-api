from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class CommentCreate(BaseModel):
    body: str = Field(min_length=1, max_length=5000)
    parent_id: UUID | None = None


class CommentRead(BaseModel):
    id: UUID
    author_id: UUID
    parent_id: UUID | None
    body: str
    created_at: datetime
