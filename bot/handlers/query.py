from aiogram import Router, F
from aiogram.types import Message
from loguru import logger

from bot.clients.api_client import APIClient

router = Router()


@router.message(F.text & ~F.text.startswith("/"))
async def handle_query(message: Message, api_client: APIClient):
    chat_id = message.chat.id
    user_query = message.text
    
    if not user_query or not user_query.strip():
        await message.answer("Пожалуйста, отправьте текстовый запрос.")
        return
    
    logger.info(f"User {chat_id} sent query: {user_query[:100]}...")
    
    result = await api_client.process_query(user_query)
    
    if result is not None:
        await message.answer(f"{result}")
        logger.info(f"Query processed successfully for user {chat_id}, result: {result}")
    else:
        await message.answer(
            "Произошла ошибка при обработке запроса. "
            "Попробуйте переформулировать запрос или повторить позже."
        )
        logger.error(f"Failed to process query for user {chat_id}")

