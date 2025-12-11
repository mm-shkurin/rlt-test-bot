from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message
from loguru import logger

from bot.clients.api_client import APIClient
from bot.core.config import BotSettings

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, api_client: APIClient):
    chat_id = message.chat.id
    
    logger.info(f"User {chat_id} started the bot")
    
    user_id = await api_client.create_telegram_user(telegram_chat_id=chat_id)
    
    if user_id:
        await message.answer(
            f"Привет! Вы успешно авторизованы."
        )
        logger.info(f"User {chat_id} authorized with ID {user_id}")
    else:
        await message.answer(
            "Произошла ошибка при авторизации. Попробуйте позже."
        )
        logger.error(f"Failed to authorize user {chat_id}")

