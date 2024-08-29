import asyncio
import aiohttp
from config import CONFIG
from infi.clickhouse_orm.database import Database
from main import handle_depth_stream
from main import AssetType
from main import depth_stream_url
from model import DiffDepthStreamDispatcher, Logger, LoggingLevel


async def wss_demo(url):
    session = aiohttp.ClientSession()
    while True:
        async with session.ws_connect(url) as ws:
            await ws.send_str('{"hello": "world"}')

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    msg_json = msg.json()

                    await asyncio.sleep(1)
                    await ws.send_str('{"hello": "world"}')
                elif msg.type == aiohttp.WSMsgType.PING:
                    print("We got PING !")
                    print(msg.data)
                    print("=============================")
                    print(msg.json())
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    print("We got CLOSE !")
                    print(msg.data)
                    print("=============================")
                    print(msg.json())

                    break


def main():
    symbol = CONFIG.symbols[3][4:]

    database = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    logger = Logger(database)
    asset_type = AssetType.USD_M
    dispatcher = DiffDepthStreamDispatcher(database, logger)
    logger.log_msg("Starting wss demo loop...", LoggingLevel.INFO)

    loop = asyncio.get_event_loop()

    url = depth_stream_url(symbol, AssetType.USD_M)

    loop.run_until_complete(wss_demo(url))
    # loop.run_forever()


if __name__ == "__main__":
    main()
