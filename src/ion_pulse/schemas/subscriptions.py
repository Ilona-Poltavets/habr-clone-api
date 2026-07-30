from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class AuthorSubscriptionRead(BaseModel):
    author_id: UUID
    display_name: str
    subscribed_at: datetime


class GameCreate(BaseModel):
    slug: str = Field(pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=120)
    title: str = Field(min_length=1, max_length=240)


class GameRead(BaseModel):
    id: UUID
    slug: str
    title: str


class GameSubscriptionRead(GameRead):
    subscribed_at: datetime
