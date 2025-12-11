from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import Depends, HTTPException, Header, status
from fastapi import Request
from fastapi.security import APIKeyHeader
import jwt
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import Optional
from app.core.config import JWTSettings, TelegramSettings
from app.db.database import get_db
from app.models.users import Users
from app.schemas.token import Token  
from app.services.telegram_service import TelegramService

auth_scheme = APIKeyHeader(name="Authorization", scheme_name="Bearer", auto_error=False)

jwt_settings = JWTSettings()
telegram_settings = TelegramSettings()


async def verify_bot_token(x_bot_token: Optional[str] = Header(None, alias="X-Bot-Token")) -> str:
    if not x_bot_token or x_bot_token != telegram_settings.bot_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid bot token",
        )
    return x_bot_token

async def create_access_token(to_encode: dict):
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=jwt_settings.access_token_expire_minutes
    )
    payload = dict(to_encode)
    payload.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(
        payload, jwt_settings.secret_key, algorithm=jwt_settings.algorithm
    )

    return encoded_jwt

async def create_refresh_token(to_encode: dict):
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=jwt_settings.refresh_token_expire_minutes
    )
    payload = dict(to_encode)
    payload.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(
        payload, jwt_settings.refresh_token_secret_key, algorithm=jwt_settings.algorithm
    )

    return encoded_jwt

async def verify_token(token:str, secret_key:str, algorithm:str):
    try:
        payload = jwt.decode(token, secret_key, algorithms=[algorithm])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")

async def refresh_access_token(refresh_token: str):
    try:
        refresh_token_payload = await verify_token(
            refresh_token, 
            jwt_settings.refresh_token_secret_key, 
            jwt_settings.algorithm
        )
        if refresh_token_payload.get("type") != "refresh":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
        user_id = refresh_token_payload.get("id")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
        
        access_token_payload = {
            "id": str(user_id),  
        }
        
        new_access_token = await create_access_token(access_token_payload)
        return new_access_token
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=401, detail="Could not validate credentials")

async def get_current_user(request: Request, token: str = Depends(auth_scheme), db: AsyncSession = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    if not token:
        raise credentials_exception
    
    try:
        if token.startswith("Bearer "):
            token = token[7:]
        
        payload = await verify_token(token, jwt_settings.secret_key, jwt_settings.algorithm)
        user_id: str = payload.get("id")
        if user_id is None:
            raise credentials_exception
        if payload.get("type") != "access":
            raise credentials_exception
    except Exception:
        raise credentials_exception
    
    try:
        user_uuid = UUID(user_id)
    except (ValueError, TypeError):
        raise credentials_exception
    
    result = await db.execute(select(Users).where(Users.id == user_uuid))
    user = result.scalar_one_or_none()
    
    if user is None:
        raise credentials_exception
    
    return user

async def get_user_from_telegram(
    request: Request,
    x_bot_token: Optional[str] = Header(None, alias="X-Bot-Token"),
    x_telegram_chat_id: Optional[int] = Header(None, alias="X-Telegram-Chat-Id"),
    db: AsyncSession = Depends(get_db),
) -> Users:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate telegram credentials",
    )

    if not x_bot_token or x_bot_token != telegram_settings.bot_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid bot token",
        )

    if not x_telegram_chat_id:
        raise credentials_exception

    telegram_service = TelegramService(db)
    user = await telegram_service.get_user_by_chat_id_full(x_telegram_chat_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Telegram chat is not linked to any user",
        )

    return user

async def get_user(
    request: Request,
    token: str = Depends(auth_scheme),
    x_bot_token: Optional[str] = Header(None, alias="X-Bot-Token"),
    x_telegram_chat_id: Optional[int] = Header(None, alias="X-Telegram-Chat-Id"),
    db: AsyncSession = Depends(get_db),
) -> Users:
    if x_bot_token:
        return await get_user_from_telegram(request, x_bot_token, x_telegram_chat_id, db)
    else:
        return await get_current_user(request, token, db)