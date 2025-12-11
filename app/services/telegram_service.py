from typing import Optional, Tuple
from uuid import UUID

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.users import Users


class TelegramService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_user_by_chat_id(self, telegram_chat_id: int) -> Optional[UUID]:
        result = await self.db.execute(
            select(Users).where(Users.telegram_chat_id == telegram_chat_id)
        )
        user = result.scalar_one_or_none()
        return user.id if user else None

    async def get_user_by_chat_id_full(self, telegram_chat_id: int) -> Optional[Users]:
        result = await self.db.execute(
            select(Users).where(Users.telegram_chat_id == telegram_chat_id)
        )
        return result.scalar_one_or_none()

    async def create_or_get_telegram_user(
        self,
        telegram_chat_id: int,
        telegram_user_id: Optional[int] = None,
        telegram_username: Optional[str] = None,
    ) -> Tuple[Users, bool]:
        user = await self.get_user_by_chat_id_full(telegram_chat_id)
        
        if user:
            logger.info(f"Found existing user {user.id} for chat {telegram_chat_id}")
            return user, False
        
        logger.info(f"Creating new user for chat {telegram_chat_id}")
        
        new_user = Users(
            telegram_chat_id=telegram_chat_id,
            is_active=True,
        )
        
        self.db.add(new_user)
        await self.db.commit()
        await self.db.refresh(new_user)
        
        logger.info(f"Created new user {new_user.id} for chat {telegram_chat_id}")
        
        return new_user, True
