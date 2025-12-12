import asyncio
import sys
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.database import get_async_sessionmaker
from app.services.data_loader_service import DataLoaderService


async def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/load_data.py <path_to_json_file>")
        sys.exit(1)
    
    json_file_path = sys.argv[1]
    
    if not Path(json_file_path).exists():
        logger.error(f"File not found: {json_file_path}")
        sys.exit(1)
    
    logger.info("Starting data loading process...")
    
    try:
        sessionmaker = get_async_sessionmaker()
        
        async with sessionmaker() as session:
            loader = DataLoaderService(session)
            result = await loader.load_from_json_file(json_file_path)
            
            logger.success(
                f"Data loading completed successfully!\n"
                f"  Videos loaded: {result['videos']}\n"
                f"  Snapshots loaded: {result['snapshots']}"
            )
            
    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

