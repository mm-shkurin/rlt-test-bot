from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from bot.clients.api_client import APIClient


class APIClientMiddleware(BaseMiddleware):
    def __init__(self, api_client: APIClient):
        self.api_client = api_client
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        data["api_client"] = self.api_client
        return await handler(event, data)

