import asyncio
import aiohttp
from config import CONFIG
from infi.clickhouse_orm.database import Database
from main import handle_depth_stream
from main import AssetType
from model import DiffDepthStreamDispatcher, Logger, LoggingLevel


def main():
    symbol = CONFIG.symbols[3]
    session = aiohttp.ClientSession()
    database = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    logger = Logger(database)
    dispatcher = DiffDepthStreamDispatcher(database, logger)

    logger.log_msg("Starting wss demo loop...", LoggingLevel.INFO)

    loop = asyncio.get_event_loop()
    loop.create_task(
        handle_depth_stream(
            symbol[4:],
            session,
            dispatcher,
            database,
            logger,
            loop,
            AssetType.USD_M,
        )
    )
    loop.run_forever()


if __name__ == "__main__":
    main()
