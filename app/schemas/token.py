from typing import Optional

from pydantic import BaseModel, Field

class Token(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"