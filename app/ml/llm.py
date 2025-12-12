import json
from typing import Dict, Any

from gigachat import GigaChat
from loguru import logger

from app.core.config import GigaChatSettings


class LLMService:
    def __init__(self, settings: GigaChatSettings):
        self.settings = settings
        self.client = GigaChat(
            credentials=settings.giga_auth_key,
            scope=settings.giga_scope,
            verify_ssl_certs=settings.giga_oauth_url.hostname != "ngw.devices.sberbank.ru"
        )
        self._schema_description = self._build_schema_description()
        self._prompt_template = self._build_prompt_template()
    
    def _build_schema_description(self) -> str:
        return """
Таблица videos (итоговая статистика по видео):
- id: UUID, идентификатор видео
- creator_id: String, идентификатор креатора
- video_created_at: DateTime, дата и время публикации видео
- views_count: Integer, финальное количество просмотров
- likes_count: Integer, финальное количество лайков
- comments_count: Integer, финальное количество комментариев
- reports_count: Integer, финальное количество жалоб
- created_at: DateTime, служебное поле
- updated_at: DateTime, служебное поле

Таблица video_snapshots (почасовые замеры статистики):
- id: String, идентификатор снапшота
- video_id: UUID, ссылка на видео (ForeignKey -> videos.id)
- views_count: Integer, текущее количество просмотров на момент замера
- likes_count: Integer, текущее количество лайков на момент замера
- comments_count: Integer, текущее количество комментариев на момент замера
- reports_count: Integer, текущее количество жалоб на момент замера
- delta_views_count: Integer, приращение просмотров с прошлого замера
- delta_likes_count: Integer, приращение лайков с прошлого замера
- delta_comments_count: Integer, приращение комментариев с прошлого замера
- delta_reports_count: Integer, приращение жалоб с прошлого замера
- created_at: DateTime, время замера (раз в час)
- updated_at: DateTime, служебное поле

Связи:
- video_snapshots.video_id -> videos.id (один ко многим)

Логика использования таблиц:
- videos: используй для итоговой статистики, подсчета видео, фильтрации по дате публикации (video_created_at)
- video_snapshots: используй для динамики, приращений (delta_*), фильтрации по дате замера (created_at)
"""
    
    def _build_prompt_template(self) -> str:
        return f"""Ты помощник для преобразования запросов на естественном языке в структурированные запросы к базе данных.

ВАЖНО: Разрешены ТОЛЬКО запросы на чтение данных (SELECT). Запрещены любые операции изменения данных: INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER и т.д.

{self._schema_description}

Типы запросов (только чтение):
- count: подсчет количества записей
- sum: сумма значений поля (используй для delta_* полей)
- distinct_count: подсчет уникальных значений

Примеры запросов и ответов:

1. "Сколько всего видео есть в системе?"
Ответ: {{"query_type": "count", "table": "videos"}}

2. "Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
Ответ: {{"query_type": "count", "table": "videos", "filters": {{"creator_id": "abc123", "date_from": "2025-11-01", "date_to": "2025-11-05"}}, "date_field": "video_created_at"}}

3. "Сколько видео набрало больше 100000 просмотров за всё время?"
Ответ: {{"query_type": "count", "table": "videos", "filters": {{"metric_gt": {{"field": "views_count", "value": 100000}}}}}}

4. "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
Ответ: {{"query_type": "sum", "table": "video_snapshots", "field": "delta_views_count", "filters": {{"date": "2025-11-28"}}, "date_field": "created_at"}}

5. "Сколько разных видео получали новые просмотры 27 ноября 2025?"
Ответ: {{"query_type": "distinct_count", "table": "video_snapshots", "field": "video_id", "filters": {{"date": "2025-11-27", "delta_views_count_gt": 0}}, "date_field": "created_at"}}

Правила:
- Для дат используй формат YYYY-MM-DD
- Если указан диапазон дат, используй date_from и date_to
- Если указана одна дата, используй date
- Для таблицы videos используй date_field: "video_created_at"
- Для таблицы video_snapshots используй date_field: "created_at"
- Для фильтрации по метрикам используй metric_gt, metric_lt, metric_eq
- Для фильтрации по приращениям используй delta_*_gt, delta_*_lt, delta_*_eq

Безопасность:
- Разрешены ТОЛЬКО запросы на чтение (count, sum, distinct_count)
- Запрещены любые операции изменения или удаления данных
- Если запрос требует изменения данных, верни ошибку в формате: {{"error": "Операция не разрешена"}}

Запрос пользователя: {{user_query}}

Верни ТОЛЬКО валидный JSON без дополнительного текста.
"""
    
    def parse_query(self, user_query: str) -> Dict[str, Any]:
        try:
            prompt = self._prompt_template.format(user_query=user_query)
            
            logger.debug(f"Sending query to LLM: {user_query[:100]}...")
            
            response = self.client.chat(prompt)
            content = response.choices[0].message.content.strip()
            
            logger.debug(f"LLM response: {content[:200]}...")
            
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "").strip()
            elif content.startswith("```"):
                content = content.replace("```", "").strip()
            
            parsed = json.loads(content)
            
            validated = self._validate_query_structure(parsed)
            
            logger.info(f"Successfully parsed query: {validated.get('query_type')} on {validated.get('table')}")
            
            return validated
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}")
            logger.error(f"Response content: {content}")
            raise ValueError(f"LLM вернул невалидный JSON: {e}")
        except Exception as e:
            logger.exception(f"Error in LLM service: {e}")
            raise
    
    def _validate_query_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise ValueError("Query must be a dictionary")
        
        if "error" in data:
            error_msg = data.get("error", "Операция не разрешена")
            raise ValueError(error_msg)
        
        dangerous_keywords = ["delete", "drop", "truncate", "alter", "insert", "update", "create", "modify"]
        data_str = json.dumps(data, ensure_ascii=False).lower()
        for keyword in dangerous_keywords:
            if keyword in data_str:
                raise ValueError(f"Обнаружена запрещенная операция: {keyword}")
        
        query_type = data.get("query_type")
        if query_type not in ["count", "sum", "distinct_count"]:
            raise ValueError(f"Invalid query_type: {query_type}. Разрешены только: count, sum, distinct_count")
        
        table = data.get("table")
        if table not in ["videos", "video_snapshots"]:
            raise ValueError(f"Invalid table: {table}. Разрешены только: videos, video_snapshots")
        
        if query_type in ["sum", "distinct_count"]:
            if "field" not in data:
                raise ValueError(f"Field is required for query_type: {query_type}")
        
        filters = data.get("filters", {})
        if not isinstance(filters, dict):
            raise ValueError("Filters must be a dictionary")
        
        date_field = data.get("date_field")
        if date_field and date_field not in ["video_created_at", "created_at"]:
            raise ValueError(f"Invalid date_field: {date_field}. Разрешены только: video_created_at, created_at")
        
        return data

