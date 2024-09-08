import json
from pathlib import Path
from pydantic import BaseSettings, BaseModel
from typing import Dict, Any, List, Optional
from config import CONFIG
from main import AssetType
import asyncio
import aiohttp


def depth_stream_url(symbol: str, asset_type: AssetType) -> str:
    speed = CONFIG.stream_interval
    assert speed in (1000, 100), "speed must be 1000 or 100"

    symbol = symbol.lower()
    endpoint = f"{symbol}@depth" if speed == 1000 else f"{symbol}@depth@100ms"

    if asset_type == AssetType.SPOT:
        print(f"wss://stream.binance.com:9443/ws/{endpoint}")
        return f"wss://stream.binance.com:9443/ws/{endpoint}"
    elif asset_type == AssetType.USD_M:
        print(f"wss://fstream.binance.com/ws/{endpoint}")
        return f"wss://fstream.binance.com/ws/{endpoint}"
    elif asset_type == AssetType.COIN_M:
        print(f"wss://dstream.binance.com/ws/{endpoint}")
        return f"wss://dstream.binance.com/ws/{endpoint}"


class DiffDepthStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    s: str  # Symbol
    U: int  # start update id
    u: int  # end update id
    b: List[List[float]]  # bids [price, quantity]
    a: List[List[float]]  # asks [price, quantity]
    pu: Optional[int]


async def play():
    symnol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    session = aiohttp.ClientSession()

    while True:
        async with session.ws_connect(
            "wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms"
            # "wss://fstream.binance.com/ws/btcusdt@depth@100ms"
            # depth_stream_url(symnol, asset_type)
        ) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    # print(DiffDepthStreamMsg(**msg.json()))
                    print(msg.json())


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(play())
    loop.run_forever()


if __name__ == "__main__":
    main()
