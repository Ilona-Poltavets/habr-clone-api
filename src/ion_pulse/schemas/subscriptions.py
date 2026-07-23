from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class AuthorSubscriptionRead(BaseModel):
    author_id: UUID
    display_name: str
    subscribed_at: datetime
