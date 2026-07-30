from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class PublicationAiReviewRead(BaseModel):
    id: UUID
    source_revision: int
    status: str
    decision: str | None
    risk_categories: list[str] | None
    reasons: list[str] | None
    confidence: float | None
    age_rating: int | None
    provider: str | None
    model: str | None
    rules_version: str | None
    created_at: datetime
    completed_at: datetime | None
