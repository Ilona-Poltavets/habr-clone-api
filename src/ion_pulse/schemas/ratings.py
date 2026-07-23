from pydantic import BaseModel, Field


class PublicationRatingUpdate(BaseModel):
    value: int = Field(ge=1, le=5)


class PublicationRatingRead(BaseModel):
    value: int
