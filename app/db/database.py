from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool 

from app.core.config import DatabaseSettings

Base = declarative_base()

class BaseModel(Base):
    __abstract__ = True

_engine = None
_AsyncSessionLocal = None

def get_engine():
    global _engine
    if _engine is None:
        db_settings = DatabaseSettings()
        _engine = create_async_engine(
            db_settings.database_url,
            future=True,
            poolclass=NullPool,  
        )
    return _engine

def get_async_sessionmaker():
    global _AsyncSessionLocal
    if _AsyncSessionLocal is None:
        _AsyncSessionLocal = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _AsyncSessionLocal

def get_db_session():
    return get_async_sessionmaker()()

async def get_db() -> AsyncSession:
    async with get_async_sessionmaker()() as session:
        yield session