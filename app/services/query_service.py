from datetime import datetime, timedelta
from typing import Any, Dict
import re

import dateparser
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.videos import Video
from app.models.video_snapshots import VideoSnapshot


class QueryService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def execute_query(self, query_params: Dict[str, Any]) -> int:
        query_type = query_params.get("query_type")
        table = query_params.get("table")
        
        if query_type == "count":
            return await self._execute_count(query_params, table)
        elif query_type == "sum":
            return await self._execute_sum(query_params, table)
        elif query_type == "distinct_count":
            return await self._execute_distinct_count(query_params, table)
        else:
            raise ValueError(f"Unknown query_type: {query_type}")
    
    async def _execute_count(self, query_params: Dict[str, Any], table: str) -> int:
        if table == "videos":
            stmt = select(func.count(Video.id))
            model = Video
        else:
            stmt = select(func.count(VideoSnapshot.id))
            model = VideoSnapshot
        
        stmt = self._apply_filters(stmt, query_params, model)
        
        result = await self.db.execute(stmt)
        count = result.scalar_one()
        
        logger.debug(f"Count query result: {count}")
        return int(count) if count is not None else 0
    
    async def _execute_sum(self, query_params: Dict[str, Any], table: str) -> int:
        field_name = query_params.get("field")
        if not field_name:
            raise ValueError("Field is required for sum query")
        
        if table == "videos":
            model = Video
        else:
            model = VideoSnapshot
        
        field = getattr(model, field_name, None)
        if field is None:
            raise ValueError(f"Field {field_name} not found in {table}")
        
        filters = query_params.get("filters", {})
        needs_join = table == "video_snapshots" and "creator_id" in filters
        
        if needs_join:
            stmt = select(func.sum(field)).select_from(VideoSnapshot).join(
                Video, VideoSnapshot.video_id == Video.id
            )
            stmt = self._apply_filters(stmt, query_params, model, join_already_done=True)
        else:
            stmt = select(func.sum(field))
            stmt = self._apply_filters(stmt, query_params, model)
        
        logger.debug(f"SQL query: {stmt}")
        result = await self.db.execute(stmt)
        sum_value = result.scalar_one() or 0
        
        logger.debug(f"Sum query result: {sum_value}")
        return int(sum_value)
    
    async def _execute_distinct_count(self, query_params: Dict[str, Any], table: str) -> int:
        field_name = query_params.get("field")
        if not field_name:
            raise ValueError("Field is required for distinct_count query")
        
        if table == "videos":
            model = Video
        else:
            model = VideoSnapshot
        
        field = getattr(model, field_name, None)
        if field is None:
            raise ValueError(f"Field {field_name} not found in {table}")
        
        stmt = select(func.count(func.distinct(field)))
        stmt = self._apply_filters(stmt, query_params, model)
        
        result = await self.db.execute(stmt)
        count = result.scalar_one()
        
        logger.debug(f"Distinct count query result: {count}")
        return int(count) if count is not None else 0
    
    def _apply_filters(self, stmt, query_params: Dict[str, Any], model, join_already_done: bool = False) -> Any:
        filters = query_params.get("filters", {})
        date_field = query_params.get("date_field")
        if not date_field and "date_field" in filters:
            date_field = filters.pop("date_field")
        
        if "creator_id" in filters:
            creator_id = filters["creator_id"]
            creator_id = creator_id.replace("-", "")
            if hasattr(model, "creator_id"):
                stmt = stmt.where(model.creator_id == creator_id)
            elif model == VideoSnapshot:
                if not join_already_done:
                    stmt = stmt.join(Video, VideoSnapshot.video_id == Video.id)
                stmt = stmt.where(Video.creator_id == creator_id)
                logger.debug(f"Applied creator_id filter: {creator_id}")
                query_type = query_params.get("query_type")
                field_name = query_params.get("field", "")
                if query_type == "sum" and field_name.startswith("delta_") and field_name.endswith("_count"):
                    field = getattr(VideoSnapshot, field_name, None)
                    if field:
                        stmt = stmt.where(field > 0)
                        logger.debug(f"Applied filter: {field_name} > 0")
        
        if "date" in filters:
            date_str = filters["date"]
            parsed_date = self._parse_date(date_str)
            
            if "time_from" in filters and "time_to" in filters:
                time_from_str = filters["time_from"]
                time_to_str = filters["time_to"]
                from datetime import time as dt_time
                
                try:
                    time_from_parts = time_from_str.split(":")
                    time_to_parts = time_to_str.split(":")
                    time_from = dt_time(int(time_from_parts[0]), int(time_from_parts[1]) if len(time_from_parts) > 1 else 0)
                    time_to = dt_time(int(time_to_parts[0]), int(time_to_parts[1]) if len(time_to_parts) > 1 else 0)
                    
                    date_start = datetime.combine(parsed_date.date(), time_from)
                    date_end = datetime.combine(parsed_date.date(), time_to)
                    if date_end < date_start:
                        date_end = datetime.combine(parsed_date.date() + timedelta(days=1), time_to)
                except (ValueError, IndexError):
                    date_start = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    date_end = parsed_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                date_start = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = parsed_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            if date_field == "video_created_at":
                stmt = stmt.where(
                    model.video_created_at >= date_start,
                    model.video_created_at <= date_end
                )
                logger.debug(f"Applied date filter (video_created_at): {date_start} - {date_end}")
            elif date_field == "created_at":
                stmt = stmt.where(
                    VideoSnapshot.created_at >= date_start,
                    VideoSnapshot.created_at <= date_end
                )
                logger.debug(f"Applied date filter (created_at): {date_start} - {date_end}")
        
        if "date_from" in filters and "date_to" in filters:
            date_from = self._parse_date(filters["date_from"])
            date_to = self._parse_date(filters["date_to"])
            
            date_from_start = date_from.replace(hour=0, minute=0, second=0, microsecond=0)
            date_to_end = date_to.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            if not date_field:
                if model == Video:
                    date_field = "video_created_at"
                elif model == VideoSnapshot:
                    date_field = "created_at"
            
            if date_field == "video_created_at":
                stmt = stmt.where(
                    model.video_created_at >= date_from_start,
                    model.video_created_at <= date_to_end
                )
                logger.debug(f"Applied date range filter (video_created_at): {date_from_start} - {date_to_end}")
            elif date_field == "created_at":
                stmt = stmt.where(
                    VideoSnapshot.created_at >= date_from_start,
                    VideoSnapshot.created_at <= date_to_end
                )
                logger.debug(f"Applied date range filter (created_at): {date_from_start} - {date_to_end}")
            else:
                logger.warning(f"date_field is not set or invalid: {date_field}, filters: {filters}")
        
        if "metric_gt" in filters:
            metric_filter = filters["metric_gt"]
            field_name = metric_filter.get("field")
            value = metric_filter.get("value")
            field = getattr(model, field_name, None)
            if field:
                stmt = stmt.where(field > value)
        
        if "metric_lt" in filters:
            metric_filter = filters["metric_lt"]
            field_name = metric_filter.get("field")
            value = metric_filter.get("value")
            field = getattr(model, field_name, None)
            if field:
                stmt = stmt.where(field < value)
        
        if "metric_eq" in filters:
            metric_filter = filters["metric_eq"]
            field_name = metric_filter.get("field")
            value = metric_filter.get("value")
            field = getattr(model, field_name, None)
            if field:
                stmt = stmt.where(field == value)
        
        delta_fields = ["delta_views_count", "delta_likes_count", "delta_comments_count", "delta_reports_count"]
        for delta_field in delta_fields:
            if f"{delta_field}_gt" in filters:
                value = filters[f"{delta_field}_gt"]
                field = getattr(model, delta_field, None)
                if field:
                    stmt = stmt.where(field > value)
            
            if f"{delta_field}_lt" in filters:
                value = filters[f"{delta_field}_lt"]
                field = getattr(model, delta_field, None)
                if field:
                    stmt = stmt.where(field < value)
            
            if f"{delta_field}_eq" in filters:
                value = filters[f"{delta_field}_eq"]
                field = getattr(model, delta_field, None)
                if field:
                    stmt = stmt.where(field == value)
        
        return stmt
    
    def _parse_date(self, date_str: str) -> datetime:
        if isinstance(date_str, datetime):
            return date_str
        
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            pass
        
        parsed = dateparser.parse(date_str, languages=["ru"])
        if parsed is None:
            raise ValueError(f"Не удалось распарсить дату: {date_str}")
        
        return parsed

