import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from uuid import UUID

from dateutil import parser as date_parser
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.videos import Video
from app.models.video_snapshots import VideoSnapshot


class DataLoaderService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def load_from_json_file(self, json_file_path: str) -> Dict[str, int]:
        file_path = Path(json_file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_file_path}")
        
        logger.info(f"Loading data from {json_file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        videos_data = data.get('videos', [])
        
        if not videos_data:
            logger.warning("No videos found in JSON file")
            return {'videos': 0, 'snapshots': 0}
        
        videos_loaded = 0
        snapshots_loaded = 0
        
        batch_size = 100
        video_batch = []
        snapshot_batch = []
        
        for video_data in videos_data:
            try:
                video_id = UUID(video_data['id'])
                existing_video = await self.db.execute(
                    select(Video).where(Video.id == video_id)
                )
                if existing_video.scalar_one_or_none():
                    logger.debug(f"Video {video_id} already exists, skipping")
                    continue
                
                video = Video(
                    id=video_id,
                    creator_id=video_data['creator_id'],
                    video_created_at=self._parse_datetime(video_data['video_created_at']),
                    views_count=video_data.get('views_count', 0),
                    likes_count=video_data.get('likes_count', 0),
                    comments_count=video_data.get('comments_count', 0),
                    reports_count=video_data.get('reports_count', 0),
                    created_at=self._parse_datetime(video_data['created_at']),
                    updated_at=self._parse_datetime(video_data['updated_at']),
                )
                video_batch.append(video)
                snapshots_data = video_data.get('snapshots', [])
                for snapshot_data in snapshots_data:
                    snapshot = VideoSnapshot(
                        id=snapshot_data['id'],
                        video_id=video_id,
                        views_count=snapshot_data.get('views_count', 0),
                        likes_count=snapshot_data.get('likes_count', 0),
                        comments_count=snapshot_data.get('comments_count', 0),
                        reports_count=snapshot_data.get('reports_count', 0),
                        delta_views_count=snapshot_data.get('delta_views_count', 0),
                        delta_likes_count=snapshot_data.get('delta_likes_count', 0),
                        delta_comments_count=snapshot_data.get('delta_comments_count', 0),
                        delta_reports_count=snapshot_data.get('delta_reports_count', 0),
                        created_at=self._parse_datetime(snapshot_data['created_at']),
                        updated_at=self._parse_datetime(snapshot_data['updated_at']),
                    )
                    snapshot_batch.append(snapshot)
                
                if len(video_batch) >= batch_size:
                    await self._commit_batch(video_batch, snapshot_batch)
                    videos_loaded += len(video_batch)
                    snapshots_loaded += len(snapshot_batch)
                    video_batch = []
                    snapshot_batch = []
                    logger.info(f"Loaded batch: {videos_loaded} videos, {snapshots_loaded} snapshots")
                    
            except Exception as e:
                logger.error(f"Error processing video {video_data.get('id', 'unknown')}: {e}")
                continue
        
        if video_batch:
            await self._commit_batch(video_batch, snapshot_batch)
            videos_loaded += len(video_batch)
            snapshots_loaded += len(snapshot_batch)
        
        logger.info(f"Data loading completed: {videos_loaded} videos, {snapshots_loaded} snapshots")
        return {'videos': videos_loaded, 'snapshots': snapshots_loaded}
    
    async def _commit_batch(
        self, 
        video_batch: List[Video], 
        snapshot_batch: List[VideoSnapshot]
    ) -> None:
        try:
            if video_batch:
                self.db.add_all(video_batch)
                await self.db.commit()
                for video in video_batch:
                    await self.db.refresh(video)
            
            if snapshot_batch:
                self.db.add_all(snapshot_batch)
                await self.db.commit()
                for snapshot in snapshot_batch:
                    await self.db.refresh(snapshot)
                
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error committing batch: {e}")
            raise
    
    def _parse_datetime(self, date_string: str) -> datetime:
        if isinstance(date_string, datetime):
            return date_string
        return date_parser.parse(date_string)

