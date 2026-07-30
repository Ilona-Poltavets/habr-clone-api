from pydantic import BaseModel, Field


class CategoryRead(BaseModel):
    slug: str
    name: str
    description: str
    color: str
    sort_order: int


class CategoryUpdate(BaseModel):
    name_ru: str = Field(min_length=1, max_length=120)
    name_en: str = Field(min_length=1, max_length=120)
    description_ru: str = Field(max_length=500)
    description_en: str = Field(max_length=500)
    color: str = Field(pattern="^#[0-9A-Fa-f]{6}$")
    sort_order: int = Field(ge=0)
    is_visible: bool


class CategoryManagementRead(CategoryUpdate):
    slug: str
