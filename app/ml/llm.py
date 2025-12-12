import json
import re
from string import Template
from typing import Dict, Any, Optional

from gigachat import GigaChat
from loguru import logger

from app.core.config import GigaChatSettings


class LLMService:
    def __init__(self, settings: GigaChatSettings):
        self.settings = settings
        oauth_url_str = str(settings.giga_oauth_url)
        verify_ssl = "ngw.devices.sberbank.ru" not in oauth_url_str
        self.client = GigaChat(
            credentials=settings.giga_auth_key,
            scope=settings.giga_scope,
            verify_ssl_certs=verify_ssl
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
        schema_desc = self._schema_description
        template_str = """Ты помощник для преобразования запросов на естественном языке в структурированные запросы к базе данных.

ВАЖНО: Разрешены ТОЛЬКО запросы на чтение данных (SELECT). Запрещены любые операции изменения данных: INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER и т.д.

$schema_description

Типы запросов (только чтение):
- count: подсчет количества записей
- sum: сумма значений поля (используй для delta_* полей)
- distinct_count: подсчет уникальных значений

Примеры запросов и ответов:

1. "Сколько всего видео есть в системе?"
Ответ: {"query_type": "count", "table": "videos"}

2. "Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
Ответ: {"query_type": "count", "table": "videos", "filters": {"creator_id": "abc123", "date_from": "2025-11-01", "date_to": "2025-11-05"}, "date_field": "video_created_at"}

3. "Сколько видео набрало больше 100000 просмотров за всё время?"
Ответ: {"query_type": "count", "table": "videos", "filters": {"metric_gt": {"field": "views_count", "value": 100000}}}

4. "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
Ответ: {"query_type": "sum", "table": "video_snapshots", "field": "delta_views_count", "filters": {"date": "2025-11-28"}, "date_field": "created_at"}

5. "Сколько разных видео получали новые просмотры 27 ноября 2025?"
Ответ: {"query_type": "distinct_count", "table": "video_snapshots", "field": "video_id", "filters": {"date": "2025-11-27", "delta_views_count_gt": 0}, "date_field": "created_at"}

6. "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 набрали больше 10000 просмотров?"
Ответ: {"query_type": "count", "table": "videos", "filters": {"creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63", "metric_gt": {"field": "views_count", "value": 10000}}}

КРИТИЧЕСКИ ВАЖНО для creator_id:
- НИКОГДА не изменяй, не добавляй и не удаляй символы в creator_id
- Копируй creator_id БУКВА В БУКВУ из запроса пользователя
- Если в запросе "aca1061a9d324ecf8c3fa2bb32d7be63", используй ТОЧНО "aca1061a9d324ecf8c3fa2bb32d7be63"
- НЕ добавляй лишние символы, НЕ исправляй, НЕ форматируй
- Это критично для корректной работы системы

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
- Если запрос требует изменения данных, верни ошибку в формате: {"error": "Операция не разрешена"}

Запрос пользователя: $user_query

Верни ТОЛЬКО валидный JSON без дополнительного текста.
"""
        template = Template(template_str)
        return template.safe_substitute(schema_description=schema_desc, user_query="$user_query")
    
    def _extract_creator_id_from_query(self, user_query: str) -> Optional[str]:
        patterns = [
            r'(?:креатора\s+с\s+id|id|creator_id)[\s:]+([a-f0-9\-]{32,36})',
            r'([a-f0-9]{32})', 
            r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', 
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, user_query, re.IGNORECASE)
            for match in matches:
                creator_id = match.group(1).replace("-", "").lower()
                if len(creator_id) == 32:  
                    if re.match(r'^[a-f0-9]{32}$', creator_id):
                        logger.debug(f"Extracted creator_id from query: {creator_id}")
                        return creator_id
        
        return None
    
    def _fix_creator_id_if_distorted(self, parsed: Dict[str, Any], original_query: str) -> Dict[str, Any]:
        filters = parsed.get("filters", {})
        if "creator_id" not in filters:
            return parsed
        
        original_creator_id = self._extract_creator_id_from_query(original_query)
        if not original_creator_id:
            return parsed
        
        llm_creator_id = str(filters["creator_id"]).replace("-", "").lower()
        original_creator_id_normalized = original_creator_id.lower()
        
        if llm_creator_id != original_creator_id_normalized:
            logger.warning(
                f"LLM distorted creator_id: original='{original_creator_id_normalized}', "
                f"llm='{llm_creator_id}'. Fixing..."
            )
            filters["creator_id"] = original_creator_id
            parsed["filters"] = filters
        else:
            filters["creator_id"] = original_creator_id
            parsed["filters"] = filters
        
        return parsed
    
    def parse_query(self, user_query: str) -> Dict[str, Any]:
        try:
            template = Template(self._prompt_template)
            prompt = template.safe_substitute(user_query=user_query)
            
            logger.debug(f"Sending query to LLM: {user_query[:100]}...")
            
            response = self.client.chat(prompt)
            content = response.choices[0].message.content.strip()
            
            logger.debug(f"LLM response: {content[:200]}...")
            
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "").strip()
            elif content.startswith("```"):
                content = content.replace("```", "").strip()
            
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            
            content = content.strip()
            
            content = re.sub(r',(\s*[}\]])', r'\1', content)
            
            parsed = json.loads(content)
            parsed = self._fix_creator_id_if_distorted(parsed, user_query)
            
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
        allowed_values = ["count", "sum", "distinct_count", "videos", "video_snapshots", "video_created_at", "created_at"]
        
        for key, value in data.items():
            key_lower = str(key).lower()
            for keyword in dangerous_keywords:
                if keyword in key_lower and key_lower not in ["date_field"]:
                    raise ValueError(f"Обнаружена запрещенная операция: {keyword} в ключе {key}")
            
            if key == "date_field":
                continue
            
            if isinstance(value, str):
                value_lower = value.lower()
                if value_lower in allowed_values:
                    continue
                for keyword in dangerous_keywords:
                    if keyword in value_lower:
                        raise ValueError(f"Обнаружена запрещенная операция: {keyword} в значении {value}")
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    sub_key_lower = str(sub_key).lower()
                    for keyword in dangerous_keywords:
                        if keyword in sub_key_lower:
                            raise ValueError(f"Обнаружена запрещенная операция: {keyword} в ключе {sub_key}")
                    
                    if isinstance(sub_value, str):
                        sub_value_lower = sub_value.lower()
                        if sub_value_lower in allowed_values:
                            continue
                        for keyword in dangerous_keywords:
                            if keyword in sub_value_lower:
                                raise ValueError(f"Обнаружена запрещенная операция: {keyword} в значении {sub_value}")
        
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

