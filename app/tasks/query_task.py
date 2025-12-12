from typing import Dict, Any

from loguru import logger

from app.core.config import GigaChatSettings
from app.db.database import get_async_sessionmaker
from app.ml.llm import LLMService
from app.services.query_service import QueryService


async def process_query_task(
    ctx: Dict[str, Any],
    user_query: str
) -> int:
    llm_settings = GigaChatSettings()
    llm_service = LLMService(llm_settings)
    
    sessionmaker = get_async_sessionmaker()
    
    try:
        async with sessionmaker() as session:
            query_params = llm_service.parse_query(user_query)
            
            query_service = QueryService(session)
            result = await query_service.execute_query(query_params)
            
            logger.info(f"Query processed successfully: {user_query[:50]}... -> {result}")
            
            return result
            
    except ValueError as e:
        logger.error(f"Validation error processing query: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error processing query: {e}")
        raise

