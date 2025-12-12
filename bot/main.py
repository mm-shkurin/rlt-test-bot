import asyncio
import sys
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger

from bot.clients.api_client import APIClient
from bot.core.config import BotSettings
from bot.core.middleware import APIClientMiddleware
from bot.handlers import start, query


async def setup_logging():
    logger.add(
        "logs/bot.log",
        rotation="1 day",
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


@asynccontextmanager
async def lifespan(bot: Bot, api_client: APIClient):
    logger.info("Bot starting...")
    yield
    logger.info("Bot shutting down...")
    await api_client.close()


async def main():
    await setup_logging()
    
    settings = BotSettings()
    
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    api_client = APIClient(settings)
    
    dp = Dispatcher()
    dp.message.middleware(APIClientMiddleware(api_client))
    dp.include_router(start.router)
    dp.include_router(query.router)
    
    async with lifespan(bot, api_client):
        logger.info("Bot is running...")
        await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

