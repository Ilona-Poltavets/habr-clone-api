from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator


class AuthorApplicationCreate(BaseModel):
    motivation: str = Field(min_length=50, max_length=2000)
    portfolio_url: HttpUrl | None = None

    @field_validator("motivation")
    @classmethod
    def normalize_motivation(cls, value: str) -> str:
        normalized = value.strip()
        if len(normalized) < 50:
            raise ValueError("Motivation must contain at least 50 non-whitespace characters")
        return normalized


class AuthorApplicationRead(BaseModel):
    id: UUID
    motivation: str
    portfolio_url: HttpUrl | None
    status: str
    created_at: datetime
    review_note: str | None = None


class AuthorApplicationDecision(BaseModel):
    status: str = Field(pattern="^(approved|rejected)$")
    review_note: str = Field(min_length=1, max_length=1000)

    @field_validator("review_note")
    @classmethod
    def require_meaningful_review_note(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Review note must not be blank")
        return normalized
