from arq.connections import RedisSettings as ArqRedisSettings

from app.core.config import RedisSettings
from app.tasks.query_task import process_query_task

app_redis_config = RedisSettings()


class WorkerSettings:
    functions = [process_query_task]
    
    redis_settings = ArqRedisSettings(
        host=app_redis_config.redis_host,
        port=app_redis_config.redis_port
    )

