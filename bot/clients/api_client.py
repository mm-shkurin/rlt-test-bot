from typing import Optional
from uuid import UUID

import httpx
from loguru import logger

from bot.core.config import BotSettings


class APIClient:
    def __init__(self, settings: BotSettings):
        self.settings = settings
        self.base_url = settings.backend_api_url
        self.bot_token = settings.bot_token
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def create_telegram_user(self, telegram_chat_id: int) -> Optional[UUID]:
        try:
            response = await self.client.post(
                f"{self.base_url}/auth/telegram/create",
                json={"telegram_chat_id": telegram_chat_id},
                headers={"X-Bot-Token": self.bot_token},
            )
            response.raise_for_status()
            data = response.json()
            return UUID(data["id"])
        except httpx.HTTPStatusError as e:
            logger.error(f"API error creating user: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.exception(f"Error creating telegram user: {e}")
            return None
    
    async def process_query(self, query: str) -> Optional[int]:
        try:
            response = await self.client.post(
                f"{self.base_url}/query/query",
                json={"query": query},
                headers={"X-Bot-Token": self.bot_token},
            )
            response.raise_for_status()
            data = response.json()
            return data.get("result")
        except httpx.HTTPStatusError as e:
            logger.error(f"API error processing query: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.exception(f"Error processing query: {e}")
            return None
    
    async def close(self):
        await self.client.aclose()

