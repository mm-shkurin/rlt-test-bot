import uuid

from sqlalchemy import BigInteger, Boolean, Column, DateTime, func
from sqlalchemy.dialects.postgresql import UUID

from app.db.database import Base


class Users(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    telegram_chat_id = Column(BigInteger, unique=True, index=True, nullable=False)
    
    is_active = Column(Boolean, default=True)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

