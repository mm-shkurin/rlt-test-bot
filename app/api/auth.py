from fastapi import APIRouter, Depends, HTTPException, Request, status
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.schemas.user import UserCreate, UserResponse
from app.services.telegram_service import TelegramService
from app.utils.security import verify_bot_token

auth_router = APIRouter()


@auth_router.post("/telegram/create", response_model=UserResponse)
async def create_telegram_user(
    request: Request,
    payload: UserCreate,
    db: AsyncSession = Depends(get_db),
    bot_token: str = Depends(verify_bot_token),
):
    telegram_service = TelegramService(db)
    
    try:
        user, is_new = await telegram_service.create_or_get_telegram_user(
            telegram_chat_id=payload.telegram_chat_id,
        )
        
        return UserResponse.model_validate(user)
    except Exception as e:
        logger.exception(f"Error creating telegram user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create telegram user",
        )
