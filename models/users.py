from typing import Optional

from pydantic import BaseModel

class User(BaseModel):
    username: str
    password: str
    role:str


class UserUpdate(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    role:Optional[str] = None
