from fastapi import APIRouter, HTTPException, status
from loguru import logger
from arq import create_pool
from arq.connections import RedisSettings as ArqRedisSettings

from app.core.config import RedisSettings
from app.schemas.query import QueryRequest, QueryResponse
from app.tasks.query_task import process_query_task

query_router = APIRouter()


async def get_arq_pool():
    redis_config = RedisSettings()
    redis_settings = ArqRedisSettings(
        host=redis_config.redis_host,
        port=redis_config.redis_port
    )
    pool = await create_pool(redis_settings)
    return pool


@query_router.post("/query", response_model=QueryResponse)
async def process_query(
    payload: QueryRequest,
):
    pool = None
    try:
        pool = await get_arq_pool()
        
        job = await pool.enqueue_job(
            "process_query_task",
            payload.query
        )
        
        result = await job.result(timeout=60)
        
        return QueryResponse(result=result)
        
    except ValueError as e:
        logger.warning(f"Query validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.exception(f"Error processing query: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process query"
        )
    finally:
        if pool:
            await pool.close()

