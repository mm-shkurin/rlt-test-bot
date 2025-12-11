from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


class UserBase(BaseModel):
    telegram_chat_id: int = Field(..., description="Telegram chat ID")
    is_active: bool = Field(default=True, description="User active status")


class UserCreate(UserBase):
    pass


class UserResponse(UserBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

