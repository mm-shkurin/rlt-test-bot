from pydantic import Field
from pydantic_settings import BaseSettings

from app.core.config import BaseConfig


class BotSettings(BaseSettings):
    bot_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")
    backend_api_url: str = Field(..., alias="BACKEND_API_URL")
    
    model_config = BaseConfig.model_config

