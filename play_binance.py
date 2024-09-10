import json
import os
from pathlib import Path
from pydantic import BaseSettings, BaseModel
from typing import Dict, Any, List, Optional
from config import CONFIG
from main import AssetType
import asyncio
import aiohttp
from aiohttp import ClientSession


class DepthSnapshotMsg(BaseModel):
    lastUpdateId: int
    bids: List[List[float]]
    asks: List[List[float]]
    E: Optional[int]
    T: Optional[int]


class DiffDepthStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    T: int  # Transaction time
    s: str  # Symbol
    U: int  # First update ID in event
    u: int  # Final update ID in event
    pu: Optional[int]  # Final update Id in last stream(ie 'u' in last stream)
    b: List[List[float]]  # bids [price, quantity]
    a: List[List[float]]  # asks [price, quantity]


def check_event_asks_bids_price_repetition(event_json):
    dict_ask = {"-100": "-100"}
    for ask in event_json["a"]:
        if ask[0] in dict_ask:
            print("Got Ask Price {ask[0]} already !!")
        else:
            dict_ask[ask[0]] = ask[1]

    dict_bid = {"-100": "-100"}
    for bid in event_json["b"]:
        if bid[0] in dict_bid:
            print("Got Bid Price {bid[0]} already !!")
        else:
            dict_bid[bid[0]] = bid[1]


def depth_stream_url(symbol: str, asset_type: AssetType, speed: int) -> str:
    # speed = CONFIG.stream_interval
    assert speed in (1000, 100), "speed must be 1000 or 100"

    symbol = symbol.lower()
    endpoint = f"{symbol}@depth" if speed == 1000 else f"{symbol}@depth@100ms"

    if asset_type == AssetType.SPOT:
        # print(f"wss://stream.binance.com:9443/ws/{endpoint}")
        return f"wss://stream.binance.com:9443/ws/{endpoint}"
    elif asset_type == AssetType.USD_M:
        # print(f"wss://fstream.binance.com/ws/{endpoint}")
        return f"wss://fstream.binance.com/ws/{endpoint}"
    elif asset_type == AssetType.COIN_M:
        # print(f"wss://dstream.binance.com/ws/{endpoint}")
        return f"wss://dstream.binance.com/ws/{endpoint}"


async def get_diff_depth_stream(speed: int = 100):
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    session = aiohttp.ClientSession()

    snapshot_interval = 2000  # per 0.1 seconds for 100ms stream
    diff_stream_to_file_interval = 100  # per 0.1 seconds for 100ms stream

    counter = 0
    list_dict = []

    prev_u = -1
    while True:
        async with session.ws_connect(
            depth_stream_url(symbol, asset_type, speed)
        ) as ws:

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    dict = msg.json()

                    if prev_u != -1 and prev_u != dict["pu"]:
                        print("prev_u != pu !!, we need new snapshot.")

                    prev_u = dict["u"]

                    list_dict.append(dict)

                    if len(list_dict) >= diff_stream_to_file_interval:
                        with open(
                            f"D:/Database/BinanceDataStreams/diff_depth_stream_{speed}ms.txt",
                            "a",
                        ) as f:
                            for dict in list_dict:
                                json.dump(dict, f)
                                f.write("\n")

                        list_dict = []

                    # print(DiffDepthStreamMsg(**dict))

                    counter += 1
                    if (counter % snapshot_interval) == 0:
                        asyncio.get_event_loop().create_task(
                            get_full_depth_snapshot(
                                symbol=symbol,
                                asset_type=asset_type,
                                counter=int(counter / snapshot_interval),
                                session=session,
                            )
                        )

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    print("ws connection closed normally")
                    break


async def get_full_depth_snapshot(
    symbol: str, asset_type: AssetType, counter: int, session: ClientSession
):
    limit = CONFIG.full_fetch_limit
    if asset_type == AssetType.SPOT:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
        # print(url)
    elif asset_type == AssetType.USD_M:
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
        # print(url)
    elif asset_type == AssetType.COIN_M:
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit={limit}"
        # print(url)

    async with session.get(url) as resp:
        resp_json = await resp.json()

        with open(
            f"D:/Database/BinanceDataStreams/depth_snapshot_{counter}.txt", "a"
        ) as f:
            json.dump(resp_json, f)
            f.write("\n")

        # msg = DepthSnapshotMsg(**resp_json)


def file_load_stream_data(
    file_path: str = "D:/Database/BinanceDataStreams",
    speed: int = 100,
    lastUpdateId_1: int = 0,
    lastUpdateId_2: int = 0,
):
    events = []
    drops = 0
    prev_u = -1
    with open(f"{file_path}/diff_depth_stream_{speed}ms.txt") as f:
        for line in f:
            event_json = json.loads(line)

            pu = event_json["pu"]
            if prev_u != -1 and pu != prev_u:
                print("Non-continuous stream, need to get new snapshot !!")

            u = event_json["u"]
            prev_u = u

            if u < lastUpdateId_1:
                drops += 1
                continue

            U = event_json["U"]
            # |U --- |snapshot| --- u|
            # The FIRST processed event should have U <= lastUpdateId **AND** u >= lastUpdateId
            if U <= lastUpdateId_1 and lastUpdateId_1 <= u:
                print(f"Got the FIRST event to process after {drops} drops...")

            events.append(event_json)

    return events


def file_load_snapshot_data(
    file_path: str = "D:/Database/BinanceDataStreams", counter: int = 1
):
    with open(f"{file_path}/depth_snapshot_{counter}.txt") as f:
        cnt_line = 0
        for line in f:
            event_json = json.loads(line)
            cnt_line += 1

        if cnt_line > 1:
            print("More than 1 line in snapshot file.")

    return event_json


def https_full_snapshot():
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        get_full_depth_snapshot(symbol=symbol, asset_type=asset_type, counter=1)
    )
    # loop.run_forever()


def wss_diff_depth_stream_and_snapshot():
    loop = asyncio.get_event_loop()
    task_100 = loop.create_task(get_diff_depth_stream(100))
    # task_1000 = loop.create_task(get_diff_depth_stream(1000))
    loop.run_until_complete(asyncio.gather(task_100))
    loop.run_forever()


if __name__ == "__main__":
    # wss_diff_depth_stream_and_snapshot()

    snapshot = file_load_snapshot_data(
        file_path="D:/Database/BinanceDataStreams", counter=1
    )

    file_load_stream_data(
        file_path="D:/Database/BinanceDataStreams",
        speed=100,
        lastUpdateId_1=snapshot["lastUpdateId"],
        lastUpdateId_2=snapshot["lastUpdateId"],
    )
