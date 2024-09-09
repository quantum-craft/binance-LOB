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


def depth_stream_url(symbol: str, asset_type: AssetType, speed: int) -> str:
    # speed = CONFIG.stream_interval
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


async def get_diff_depth_stream(speed: int = 100):
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    session = aiohttp.ClientSession()

    snapshot_interval = 2000  # milliseconds

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

                    # TODO: open file too often
                    if len(list_dict) >= 100:
                        for dict in list_dict:
                            with open(
                                f"DataStreams/diff_depth_stream_{speed}ms.txt", "a"
                            ) as f:
                                json.dump(dict, f)
                                f.write("\n")

                        list_dict = []

                    # TODO: handle the exception
                    # print(DiffDepthStreamMsg(**dict))
                    counter += 1
                    if (counter % snapshot_interval) == 0:
                        asyncio.get_event_loop().create_task(
                            get_full_depth_snapshot(
                                symbol=symbol,
                                asset_type=asset_type,
                                counter=int(counter / snapshot_interval),
                            )
                        )


async def get_full_depth_snapshot(symbol: str, asset_type: AssetType, counter: int):
    session = aiohttp.ClientSession()

    limit = CONFIG.full_fetch_limit
    if asset_type == AssetType.SPOT:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
        print(url)
    elif asset_type == AssetType.USD_M:
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
        print(url)
    elif asset_type == AssetType.COIN_M:
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit={limit}"
        print(url)

    async with session.get(url) as resp:
        resp_json = await resp.json()

        with open(f"DataStreams/depth_snapshot_{counter}.txt", "a") as f:
            json.dump(resp_json, f)
            f.write("\n")

    session.close()
    # msg = DepthSnapshotMsg(**resp_json)


def load_stream_data_from_file(speed: int = 100, lastUpdateId: int = 0):
    events = []
    drops = 0
    prev_u = -1
    with open(f"DataStreams/diff_depth_stream_{speed}ms.txt") as f:
        for line in f:
            event_json = json.loads(line)

            pu = event_json["pu"]
            if prev_u != -1 and pu != prev_u:
                # While listening to the stream, each new event's pu should be equal to the previous event's u, otherwise initialize the process from step 3.
                print("Fuck! Incorrect stream !!!!!!")

            u = event_json["u"]
            prev_u = u
            # Drop any event where u is < lastUpdateId in the snapshot.
            if u < lastUpdateId:
                drops += 1
            else:
                U = event_json["U"]
                # U ------ snapshot ------ u
                if U <= lastUpdateId and u >= lastUpdateId:
                    # The first processed event should have U <= lastUpdateId**AND**u >= lastUpdateId
                    print(f"Got event after {drops} drops!!")
                    print("Got the first event within range !!")

                events.append(event_json)

            dict_ask = {"-100": "-100"}
            for ask in event_json["a"]:
                if ask[0] in dict_ask:
                    print("Got Ask price {ask[0]} already !!")
                else:
                    dict_ask[ask[0]] = ask[1]

            dict_bid = {"-100": "-100"}
            for bid in event_json["b"]:
                if bid[0] in dict_bid:
                    print("Got Bid price {bid[0]} already !!")
                else:
                    dict_bid[bid[0]] = bid[1]

        # my_list = [json.loads(line) for line in f]
        # print(my_list)

    return events


def load_snapshot_data_from_file(counter: int = 1):
    with open(f"DataStreams/depth_snapshot_{counter}.txt") as f:
        for line in f:
            event_json = json.loads(line)
            lastUpdateId = event_json["lastUpdateId"]

    return lastUpdateId


def diff_depth_stream():
    loop = asyncio.get_event_loop()
    task_100 = loop.create_task(get_diff_depth_stream(100))
    # task_1000 = loop.create_task(get_diff_depth_stream(1000))
    loop.run_until_complete(asyncio.gather(task_100))
    loop.run_forever()


def full_snapshot():
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        get_full_depth_snapshot(symbol=symbol, asset_type=asset_type, counter=1)
    )
    # loop.run_forever()


if __name__ == "__main__":
    # diff_depth_stream()
    # full_snapshot()

    for counter in range(1, 17):
        lastUpdateId = load_snapshot_data_from_file(counter=counter)
        events = load_stream_data_from_file(speed=100, lastUpdateId=lastUpdateId)
        print(f"Event length: {len(events)}")
