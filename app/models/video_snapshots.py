from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID

from app.db.database import Base


class VideoSnapshot(Base):
    __tablename__ = "video_snapshots"

    id = Column(String, primary_key=True)
    
    video_id = Column(UUID(as_uuid=True), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False, index=True)
    
    views_count = Column(Integer, nullable=False, default=0)
    likes_count = Column(Integer, nullable=False, default=0)
    comments_count = Column(Integer, nullable=False, default=0)
    reports_count = Column(Integer, nullable=False, default=0)
    
    delta_views_count = Column(Integer, nullable=False, default=0)
    delta_likes_count = Column(Integer, nullable=False, default=0)
    delta_comments_count = Column(Integer, nullable=False, default=0)
    delta_reports_count = Column(Integer, nullable=False, default=0)
    
    created_at = Column(DateTime(timezone=True), nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

