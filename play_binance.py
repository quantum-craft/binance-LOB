import json
import os
import sys
import argparse
from pathlib import Path
from pydantic import BaseSettings, BaseModel
from typing import Dict, Any, List, Optional
from config import CONFIG
from main import AssetType
import asyncio
import aiohttp
from aiohttp import ClientSession
from sortedcontainers import SortedDict
import datetime
import time


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


async def get_partial_depth_stream(
    file_path: str = f"D:/Database/BinanceDataStreams", speed: int = 100
):
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M
    level = 10
    endpoint = f"{symbol}@depth{level}@{speed}ms"
    partial_stream_to_file_interval = 100  # per 0.1 seconds for 100ms stream

    session = aiohttp.ClientSession()

    list_dict = []
    prev_u = -1
    while True:
        async with session.ws_connect(f"wss://fstream.binance.com/ws/{endpoint}") as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    dict = msg.json()

                    if prev_u != -1 and prev_u != dict["pu"]:
                        print("prev_u != pu !!, we need new snapshot.")

                    prev_u = dict["u"]

                    list_dict.append(dict)

                    if len(list_dict) >= partial_stream_to_file_interval:
                        with open(
                            f"{file_path}/partial_depth_stream_{speed}ms.txt",
                            "a",
                        ) as f:
                            for dict in list_dict:
                                json.dump(dict, f)
                                f.write("\n")

                        list_dict = []

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    print("ws connection closed normally")
                    break


async def get_diff_depth_stream(
    file_path: str = f"D:/Database/BinanceDataStreams", speed: int = 100
):
    symbol = CONFIG.symbols[0][4:]
    asset_type = AssetType.USD_M

    session = aiohttp.ClientSession()

    snapshot_interval = 600  # per 0.1 seconds for 100ms stream
    diff_stream_to_file_interval = 100  # per 0.1 seconds for 100ms stream

    counter = 0
    list_dict = []

    start_time = time.time() * 1000
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

                    hour = int((dict["E"] - start_time) / 1000 / 3600)
                    if len(list_dict) >= diff_stream_to_file_interval:
                        with open(
                            f"{file_path}/diff_depth_stream_hour_{hour+1}_{speed}ms.txt",
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
    symbol: str,
    asset_type: AssetType,
    counter: int,
    session: ClientSession,
    file_path: str = f"D:/Database/BinanceDataStreams",
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
            f"{file_path}/depth_snapshot_{counter}.txt",
            "a",
        ) as f:
            json.dump(resp_json, f)
            f.write("\n")

        # msg = DepthSnapshotMsg(**resp_json)


def file_load_diff_stream_data(
    file_path: str = "D:/Database/BinanceDataStreams",
    speed: int = 100,
    lastUpdateId_start: int = 0,
    lastUpdateId_end: int = 0,
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

            if u < lastUpdateId_start:
                drops += 1
                continue

            U = event_json["U"]
            # |U --- |snapshot_start| --- u|   until   |U --- |snapshot_end| --- u|
            #           included                                 excluded
            # The FIRST event to process should have U <= lastUpdateId **AND** u >= lastUpdateId
            if U <= lastUpdateId_start and lastUpdateId_start <= u:
                # print(f"Got the FIRST event to process after {drops} drops...")
                pass

            events.append(event_json)

            if U <= lastUpdateId_end and lastUpdateId_end <= u:
                return events

    return events


def file_load_partial_stream_data(
    file_path: str = "D:/Database/BinanceDataStreams",
    speed: int = 100,
    lastUpdateId_start: int = 0,
    lastUpdateId_end: int = 0,
):
    events = []
    drops = 0
    prev_u = -1
    with open(f"{file_path}/partial_depth_stream_{speed}ms.txt") as f:
        for line in f:
            event_json = json.loads(line)

            pu = event_json["pu"]
            if prev_u != -1 and pu != prev_u:
                print("Non-continuous stream, need to get new snapshot !!")

            u = event_json["u"]
            prev_u = u

            if u < lastUpdateId_start:
                drops += 1
                continue

            U = event_json["U"]
            # |U --- |snapshot_start| --- u|   until   |U --- |snapshot_end| --- u|
            #           included                                 excluded
            # The FIRST event to process should have U <= lastUpdateId **AND** u >= lastUpdateId
            if U <= lastUpdateId_start and lastUpdateId_start <= u:
                print(f"Got the FIRST event to process after {drops} drops...")
                pass

            events.append(event_json)

            if U <= lastUpdateId_end and lastUpdateId_end <= u:
                return events

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


def wss_partial_depth_stream():
    loop = asyncio.get_event_loop()
    task_100 = loop.create_task(get_partial_depth_stream(100))
    # task_1000 = loop.create_task(get_diff_depth_stream(1000))
    loop.run_until_complete(asyncio.gather(task_100))
    loop.run_forever()


def wss_diff_depth_stream_and_snapshot():
    loop = asyncio.get_event_loop()
    task_100 = loop.create_task(get_diff_depth_stream(speed=100))
    # task_1000 = loop.create_task(get_diff_depth_stream(1000))
    loop.run_until_complete(asyncio.gather(task_100))
    loop.run_forever()


def get_bids_asks_from_snapshot(snapshot):
    bids_snapshot = snapshot["bids"]
    bids_book = SortedDict(lambda x: -x, {})

    asks_snapshot = snapshot["asks"]
    asks_book = SortedDict(lambda x: x, {})

    for p, v in bids_snapshot:
        bids_book[float(p)] = float(v)

    for p, v in asks_snapshot:
        asks_book[float(p)] = float(v)

    return bids_book, asks_book


def compare(
    start_counter: int,
    end_counter: int,
    file_path: str = "D:/Database/BinanceDataStreams",
):
    snapshot_start = file_load_snapshot_data(file_path=file_path, counter=start_counter)

    snapshot_end = file_load_snapshot_data(file_path=file_path, counter=end_counter)

    events_start_end = file_load_diff_stream_data(
        file_path=file_path,
        speed=100,
        lastUpdateId_start=snapshot_start["lastUpdateId"],
        lastUpdateId_end=snapshot_end["lastUpdateId"],
    )

    bids_book, asks_book = get_bids_asks_from_snapshot(snapshot_start)
    for event in events_start_end:
        process_bids_asks_book_for_event(event, bids_book, asks_book)

    events_after_end = file_load_diff_stream_data(
        file_path=file_path,
        speed=100,
        lastUpdateId_start=snapshot_end["lastUpdateId"],
        lastUpdateId_end=sys.maxsize,
    )

    bids_book_compare, asks_book_compare = get_bids_asks_from_snapshot(snapshot_end)
    process_bids_asks_book_for_event(
        events_after_end[0], bids_book_compare, asks_book_compare
    )

    prices = []
    volumes = []
    for p, v in bids_book.items():
        prices.append(p)
        volumes.append(v)

    prices_compare = []
    volumes_compare = []
    for p, v in bids_book_compare.items():
        prices_compare.append(p)
        volumes_compare.append(v)

        check_number = 1000
        prices = prices[:check_number]
        volumes = volumes[:check_number]
        prices_compare = prices_compare[:check_number]
        volumes_compare = volumes_compare[:check_number]

    for i, p in enumerate(prices):
        if p != prices_compare[i]:
            print(f"Price not equal at index {i} !!")

    for i, v in enumerate(volumes):
        if v != volumes_compare[i]:
            print(f"Volume not equal at index {i} !!")

    print("Prices and Volumes are equal !!")


def process_bids_asks_book_for_event(event, bids_book, asks_book):
    for p, v in event["b"]:
        if float(v) == 0.0000:
            bids_book.pop(float(p), default=0)
        else:
            bids_book[float(p)] = float(v)

    for p, v in event["a"]:
        if float(v) == 0.0000:
            asks_book.pop(float(p), default=0)
        else:
            asks_book[float(p)] = float(v)


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Binance Data Stream")
    # parser.add_argument("-t", "--type")

    # args = parser.parse_args()

    # if args.type == "diff":
    #     print("Recording DIFF depth stream and snapshot...")
    #     wss_diff_depth_stream_and_snapshot()
    # elif args.type == "partial":
    #     print("Recording PARTIAL depth stream...")
    #     wss_partial_depth_stream()

    # compare(1, 205, "D:\Database\BinanceDataStreams_2024_09_10")

    wss_diff_depth_stream_and_snapshot()
