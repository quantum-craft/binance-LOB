""" Replay modules.

Inlcude all useful function and classes for reconstructing orderbook from database
"""

from datetime import timedelta
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple
from infi.clickhouse_orm.database import Database
from model import DiffDepthStream, DepthSnapshot
from clickhouse_driver import Client
from config import CONFIG
from sortedcontainers import SortedDict
from itertools import chain
from dataclasses import dataclass
import numpy as np
from sklearn.preprocessing import StandardScaler
import torch
import torch.nn.functional as F


def diff_depth_stream_generator(
    last_update_id: int, symbol: str, block_size: Optional[int] = None
) -> Generator[
    Tuple[datetime, int, int, List[float], List[float], List[float], List[float], str],
    None,
    None,
]:
    database = CONFIG.db_name
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    qs = (
        DiffDepthStream.objects_in(db)
        .filter(
            DiffDepthStream.symbol == symbol.upper(),
            DiffDepthStream.final_update_id >= last_update_id,
        )
        .order_by("timestamp")
    )

    if block_size is None:
        for row in client.execute(qs.as_sql()):
            yield row
    else:
        settings = {"max_block_size": block_size}
        rows_gen = client.execute_iter(qs.as_sql(), settings=settings)
        for row in rows_gen:
            yield row


class DataBlock:
    """Data block class that represents a continuous stream of diff depth stream.

    A continuous stream of diff depth stream is a abstract collection of diff depth stream
    where the final_update_id of previous diff equals to the first_update_id - 1 of the next.

    Attributes:
        client: clickhouse driver client object
        settings: settings for SQL execution
        beginning_update_id: first update id included in the data block
        beginning_timestamp: timestamp associated with first diff depth stream
        ending_update_id: last update id included in the data block
        ending_timestamp: timestamp associated with last diff depth stream
        block_snapshot_ids: list of snapshot ids within data block
        symbol: symbol for the data block
        size: number of diff depth stream in the data block
    """

    client: Client
    settings: Dict[str, Any]
    beginning_update_id: int
    beginning_timestamp: datetime
    ending_update_id: int
    ending_timestamp: datetime
    block_snapshot_ids: List[int]
    symbol: str
    size: int

    def __init__(self, symbol: str, last_update_id: int, block_size: int = 5_000):
        self.symbol = symbol
        database = CONFIG.db_name
        self.client = Client(host=CONFIG.host_name)
        self.client.execute(f"USE {database}")
        sql = (
            "SELECT first_update_id, final_update_id FROM diffdepthstream "
            f"WHERE symbol='{symbol}' AND first_update_id>{last_update_id} "
            "ORDER BY first_update_id"
        )
        self.settings = {"max_block_size": block_size}
        rows_gen = self.client.execute_iter(sql, settings=self.settings)
        prev_update_id = None
        self.size = 0
        for row in rows_gen:
            self.size += 1
            if prev_update_id:
                first_update_id, final_update_id = row
                if prev_update_id + 1 != first_update_id:
                    self.ending_update_id = prev_update_id
                    self.size -= 1
                    break
                else:
                    prev_update_id = final_update_id
            else:
                first_update_id, prev_update_id = row
                self.beginning_update_id = first_update_id
        else:
            self.ending_update_id = prev_update_id

        if self.ending_update_id is None:
            return
        self.client.disconnect()
        self.client.execute(f"USE {database}")

        self.beginning_timestamp = self.client.execute(
            (
                "SELECT timestamp FROM diffdepthstream "
                f"WHERE first_update_id={self.beginning_update_id} AND "
                f"symbol='{symbol}'"
            )
        )[0][0]

        self.ending_timestamp = self.client.execute(
            (
                "SELECT timestamp FROM diffdepthstream "
                f"WHERE final_update_id={self.ending_update_id} AND "
                f"symbol='{symbol}'"
            )
        )[0][0]

        self.block_snapshot_ids = [
            id_
            for id_ in get_snapshots_update_ids(symbol)
            if self.beginning_update_id <= id_ + 1 <= self.ending_update_id
        ]
        self.block_snapshot_ids

        # demo_beginning_datetime = self.beginning_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        # demo_ending_datetime = self.ending_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        # print(demo_beginning_datetime, demo_ending_datetime)

    def fetch_partial_book(self, level: int = 10, block_size: int = 5_000):
        """Returns a generator for the partial book

        Args:
            level (int, optional): See `partial_book_generator` . Defaults to 10.
            block_size (int, optional): See `partial_book_generator`. Defaults to 5_000.

        Returns:
            Generator[PartialBook]: generator for the parital book
        """
        return partial_orderbook_generator(
            self.beginning_update_id - 1, self.symbol, level, block_size
        )

    def __repr__(self) -> str:
        if self.ending_update_id:
            return (
                f"Datablock(symbol='{self.symbol}', size={self.size},"
                f" {self.beginning_update_id},"
                f" {self.ending_update_id})"
            )
        else:
            return (
                f"Datablock(symbol='{self.symbol}', {self.beginning_update_id}, EMPTY)"
            )

    def __len__(self) -> int:
        return self.size


def get_all_data_blocks(
    symbol: str, last_update_id: int, block_size: int = 5_000
) -> List[DataBlock]:
    datablocks = []
    cur_block = DataBlock(symbol, last_update_id, block_size)
    while cur_block.size != 0:
        datablocks.append(cur_block)
        cur_block = DataBlock(symbol, cur_block.ending_update_id, block_size)
    return datablocks


@dataclass
class FullBook:
    """The full orderbook object"""

    timestamp: datetime
    """Timestamp for the current orderbook
    """
    last_update_id: int
    """Last update id of the current orderbook
    """
    bids: Dict[float, float]
    """Bids orderbook mapping from price to volumn
    """
    asks: Dict[float, float]
    """Asks orderbook mapping from price to volumn
    """
    symbol: str
    """Symbol of the orderbook
    """


@dataclass
class PartialBook:
    """The partial orderbook object"""

    timestamp: datetime
    """Timestamp for the current orderbook
    """
    last_update_id: int
    """Last update id of the current orderbook
    """
    book: List[float]
    """Partial order book in the following format:

```[bid_1_price, bid_1_vol, ask_1_price, ask_1_vol, bid_2_price,...,ask_n_vol]```
    where n is the level.
    """
    symbol: str
    """Symbol of the orderbook
    """


def orderbook_generator(
    last_update_id: int,
    symbol: str,
    block_size: Optional[int] = 5_000,
    return_copy: bool = True,
) -> Generator[FullBook, None, None]:
    """Generator to iterate reconstructed full orderbook from diff stream where
    each element yielded are orderbook constructed from each stream update. The iterator
    is exhausted when there is a gap in the diff depth stream (probably due to connection lost
    while logging data), i.e. the previous final_update_id + 1 != first_update_id, or there is no
    more diff stream in the database. Last recieved last_update_id can be used again to create new
    generator to construct future orderbooks.

    Args:
        last_update_id (int): target update id to begin iterator. The first item
            from the iterator will be the first snapshot with last update id that
            is strictly greater than the one applied. Sucessive item will be constructed
            with diff stream while a local orderbook is maintained.
            See the link below for detail
            https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
            for more detail.
        symbol (str): symbol for orderbook to reconstruct
        block_size (Optional[int], optional): pagniate size for executing SQL queries. None
            means all data are retrived at once. Defaults to 5000.
        return_copy (bool, optional): whether a copy of local orderbook is made when yield. Set to
            false if orderbook yielded is used in a read only manner or local orderbook might be
            corrupted, and could speedup the generator significantly. Defaults to true.

    Raises:
        ValueError: ignore

    Yields:
        FullBook: Full Orderbook object representing the reconstructed orderbook
    """
    database = CONFIG.db_name
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")

    qs = (
        DepthSnapshot.objects_in(db).filter(
            DepthSnapshot.symbol == symbol.upper(),
            DepthSnapshot.last_update_id > last_update_id,
        )
    ).order_by("timestamp")

    sql_result = client.execute(qs.as_sql())
    if len(sql_result) == 0:
        return
    snapshot = sql_result.pop(0)
    next_snapshot = sql_result.pop(0) if sql_result else None
    client.disconnect()
    (
        timestamp,
        last_update_id,
        bids_quantity,
        bids_price,
        asks_quantity,
        asks_price,
        _,
    ) = snapshot

    bids_book = lists_to_dict(bids_price, bids_quantity)
    asks_book = lists_to_dict(asks_price, asks_quantity)

    if return_copy:
        yield FullBook(
            timestamp=timestamp,
            last_update_id=last_update_id,
            bids=bids_book.copy(),
            asks=asks_book.copy(),
            symbol=symbol,
        )
    else:
        yield FullBook(
            timestamp=timestamp,
            last_update_id=last_update_id,
            bids=bids_book,
            asks=asks_book,
            symbol=symbol,
        )

    prev_final_update_id = None
    for diff_stream in diff_depth_stream_generator(last_update_id, symbol, block_size):
        # https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
        (
            timestamp,
            first_update_id,
            final_update_id,
            diff_bids_quantity,
            diff_bids_price,
            diff_asks_quantity,
            diff_asks_price,
            _,
        ) = diff_stream

        if (
            prev_final_update_id is not None
            and prev_final_update_id + 1 != first_update_id
        ):
            return
        prev_final_update_id = final_update_id

        if prev_final_update_id is None and (
            last_update_id + 1 < first_update_id or last_update_id + 1 > final_update_id
        ):
            raise ValueError()

        if next_snapshot is not None and (
            first_update_id <= next_snapshot[1] + 1 <= final_update_id
        ):
            (
                _,
                _,
                bids_quantity,
                bids_price,
                asks_quantity,
                asks_price,
                _,
            ) = next_snapshot

            bids_book.clear()
            asks_book.clear()
            bids_book.update(zip(bids_price, bids_quantity))
            asks_book.update(zip(asks_price, asks_quantity))

            next_snapshot = sql_result.pop(0) if sql_result else None

        update_book(bids_book, diff_bids_price, diff_bids_quantity)
        update_book(asks_book, diff_asks_price, diff_asks_quantity)

        if return_copy:
            yield FullBook(
                timestamp=timestamp,
                last_update_id=final_update_id,
                bids=bids_book.copy(),
                asks=asks_book.copy(),
                symbol=symbol,
            )
        else:
            yield FullBook(
                timestamp=timestamp,
                last_update_id=final_update_id,
                bids=bids_book,
                asks=asks_book,
                symbol=symbol,
            )


def partial_orderbook_generator(
    last_update_id: int, symbol: str, level: int = 10, block_size: Optional[int] = 5_000
) -> Generator[PartialBook, None, None]:
    """Similar to orderbook_generator but instead of yielding a full constructed orderbook
    while maintaining a full local orderbook, a partial orderbook with level for both bids and
    asks are yielded and only a partial orderbook is maintained. This generator should be much
    faster than orderbook_generator.

    Args:
        last_update_id (int): target update id to begin iterator. The first item
            from the iterator will be the first snapshot with last update id that
            is strictly greater than the one applied. Sucessive item will be constructed
            with diff stream while a local orderbook is maintained.
            See the link below for detail
            https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
            for more detail.
        symbol (str): symbol for orderbook to reconstruct
        level (int, optional): levels of orderbook to return. Defaults to 10.
        block_size (Optional[int], optional): pagniate size for executing SQL queries. None
            means all data are retrived at once. Defaults to 5000.

    Raises:
        ValueError: ignore

    Yields:
        PartialBook: Partial Orderbook object representing reconstructed orderbook
    """
    database = CONFIG.db_name
    db = Database(CONFIG.db_name)
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")

    qs = (
        DepthSnapshot.objects_in(db).filter(
            DepthSnapshot.symbol == symbol.upper(),
            DepthSnapshot.last_update_id > last_update_id,
        )
    ).order_by("timestamp")

    sql_result = client.execute(qs.as_sql())
    if len(sql_result) == 0:
        return
    snapshot = sql_result.pop(0)
    next_snapshot = sql_result.pop(0) if sql_result else None
    client.disconnect()
    (
        timestamp,
        last_update_id,
        bids_quantity,
        bids_price,
        asks_quantity,
        asks_price,
        _,
    ) = snapshot

    bids_book = SortedDict(lambda x: -x, lists_to_dict(bids_price, bids_quantity))
    asks_book = SortedDict(lists_to_dict(asks_price, asks_quantity))

    bids_items = bids_book.items()[:level]
    asks_items = asks_book.items()[:level]

    # Asks first then Bids
    result = [
        val for (asks, bids) in zip(asks_items, bids_items) for val in chain(asks, bids)
    ]

    yield PartialBook(
        timestamp=timestamp, last_update_id=last_update_id, book=result, symbol=symbol
    )
    prev_final_update_id = None
    for diff_stream in diff_depth_stream_generator(last_update_id, symbol, block_size):
        # https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
        (
            timestamp,
            first_update_id,
            final_update_id,
            diff_bids_quantity,
            diff_bids_price,
            diff_asks_quantity,
            diff_asks_price,
            _,
        ) = diff_stream

        if (
            prev_final_update_id is not None
            and prev_final_update_id + 1 != first_update_id
        ):
            return

        prev_final_update_id = final_update_id

        if prev_final_update_id is None and (
            last_update_id + 1 < first_update_id or last_update_id + 1 > final_update_id
        ):
            raise ValueError()

        if next_snapshot is not None and (
            first_update_id <= next_snapshot[1] + 1 <= final_update_id
        ):
            (
                _,
                _,
                bids_quantity,
                bids_price,
                asks_quantity,
                asks_price,
                _,
            ) = next_snapshot

            bids_book.clear()
            asks_book.clear()
            bids_book.update(zip(bids_price, bids_quantity))
            asks_book.update(zip(asks_price, asks_quantity))

            next_snapshot = sql_result.pop(0) if sql_result else None

        update_book(bids_book, diff_bids_price, diff_bids_quantity)
        update_book(asks_book, diff_asks_price, diff_asks_quantity)

        bids_items = bids_book.items()[:level]
        asks_items = asks_book.items()[:level]

        # Asks first then Bids
        result = [
            val
            for (asks, bids) in zip(asks_items, bids_items)
            for val in chain(asks, bids)
        ]

        yield PartialBook(
            timestamp=timestamp,
            last_update_id=final_update_id,
            book=result,
            symbol=symbol,
        )


def lists_to_dict(price: List[float], quantity: List[float]) -> Dict[float, float]:
    return {p: q for p, q in zip(price, quantity)}


def update_book(
    book: Dict[float, float], price: List[float], quantity: List[float]
) -> None:
    for p, q in zip(price, quantity):
        if q == 0:
            book.pop(p, 0)
        else:
            book[p] = q


def get_snapshots_update_ids(symbol: str) -> List[int]:
    database = CONFIG.db_name
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    return [
        id_[0]
        for id_ in client.execute(
            f"SELECT last_update_id FROM depthsnapshot WHERE symbol = '{symbol.upper()}' ORDER BY "
            "timestamp"
        )
    ]


def get_all_symbols() -> List[str]:
    database = CONFIG.db_name
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")

    return [s[0] for s in client.execute("SELECT DISTINCT symbol FROM diffdepthstream")]


def check_saved_data():
    x = None
    for hour in range(1, 10):
        load_data = np.loadtxt(f"./data/X/x_hour_{hour}.csv")
        if x is None:
            x = load_data
        else:
            x = np.concatenate((x, load_data))

    total = True
    for cnt, book in enumerate(
        partial_orderbook_generator(last_update_id=0, symbol="USD_F_BTCUSDT")
    ):
        total = total & (np.count_nonzero(x[cnt, :] == book.book) == 40)

    return total


def zscore_normalisation():
    for hour in range(1, 10):
        x = np.loadtxt(f"./data/X/x_hour_{hour}.csv")

        x_mean = np.mean(x, axis=0)
        x_std = np.std(x, axis=0)
        x_scaled_np = (x - x_mean) / x_std

        scaler = StandardScaler()
        x_scaled_skl = scaler.fit_transform(x)

        print(
            np.count_nonzero((x_scaled_np - x_scaled_skl) == 0.0)
            == x.shape[0] * x.shape[1]
        )

        np.savetxt(f"./data/X/x_zscore_hour_{hour}.csv", x_scaled_np)


def print_all_data_blocks_timestamp():
    datablocks = get_all_data_blocks("USD_F_BTCUSDT", 0)
    for block in datablocks:
        print(block)
        print(block.ending_timestamp - block.beginning_timestamp)


def check_pvpv_aabb(book, asks, bids):
    total_logic = True
    manual = []
    for level in range(10):
        manual.append(asks[level][0])  # price ask
        manual.append(asks[level][1])  # volume ask
        manual.append(bids[level][0])  # price bid
        manual.append(bids[level][1])  # volume bid

    total_logic = total_logic & (manual == book.book)
    cnt = cnt + 1

    print(total_logic)


def save_data_hourly_to_file(symbol: str = "USD_F_BTCUSDT"):
    x = None
    prev_timestamp = None
    hour = 0
    for book in partial_orderbook_generator(last_update_id=0, symbol=symbol):
        if prev_timestamp is None:
            prev_timestamp = book.timestamp

        if book.timestamp - prev_timestamp > timedelta(hours=1):
            hour = hour + 1
            prev_timestamp = book.timestamp
            np.savetxt(f"./data/X/x_hour_{hour}.csv", x)
            x = None

        if x is None:
            x = np.array(book.book, dtype=np.float64).reshape(1, -1)
        else:
            x = np.concatenate(
                (x, np.array(book.book, dtype=np.float64).reshape(1, -1))
            )

    if x is not None:
        hour = hour + 1
        np.savetxt(f"./data/X/x_hour_{hour}.csv", x)


def calculate_m_plus(mid_price, k):
    total_logic = True

    m_plus = np.zeros(mid_price.shape[0] - k)
    for idx, p_t in enumerate(zip(*[mid_price[i:] for i in range(1, k + 1)])):
        list = np.asarray(p_t, dtype=np.float64)
        m_plus[idx] = np.sum(list) / k

        sum = 0.0
        for j in range(1, k + 1):
            sum = sum + mid_price[idx + j]

        sum = sum / k

        total_logic = total_logic & (np.abs(m_plus[idx] - sum) < 1e-9)

    return m_plus, total_logic


def save_m_plus_to_file():
    ks = [5, 10, 20, 50, 100]

    for k in ks:
        for hour in range(1, 10):
            mid_price = np.loadtxt(f"./data/MidPrice/mid_price_hour_{hour}.csv")
            m_plus, total_logic = calculate_m_plus(mid_price, k)

            m_plus_file = f"./data/MPlus/m_plus_k_{k}_hour_{hour}.csv"
            np.savetxt(m_plus_file, m_plus)
            print(total_logic)


def calculate_mid_price():
    for hour in range(1, 10):
        x_zscore = np.loadtxt(f"./data/X/x_hour_{hour}.csv")
        mid_price_zscore = (x_zscore[:, 0] + x_zscore[:, 2]) / 2
        np.savetxt(f"./data/MidPrice/mid_price_hour_{hour}.csv", mid_price_zscore)


def calculate_lt():
    ks = [5, 10, 20, 50, 100]
    for k in ks:
        for hour in range(1, 10):
            mid_price = np.loadtxt(f"./data/MidPrice/mid_price_hour_{hour}.csv")
            m_plus = np.loadtxt(f"./data/MPlus/m_plus_k_{k}_hour_{hour}.csv")

            lt = (m_plus - mid_price[:-k]) / mid_price[:-k]

            np.savetxt(f"./data/Y/lt_k_{k}_hour_{hour}.csv", lt)


def calculate_y(alpha=0.002):
    zero_cnt = 0
    one_cnt = 0
    minus_one_cnt = 0

    ks = [5, 10, 20, 50, 100]
    for k in ks:
        for hour in range(1, 10):
            lt = np.loadtxt(f"./data/Y/lt_k_{k}_hour_{hour}.csv")

            y = np.zeros_like(lt)
            for i in range(lt.shape[0]):
                if lt[i] > alpha:
                    y[i] = 1
                    one_cnt = one_cnt + 1
                elif lt[i] < -alpha:
                    y[i] = -1
                    minus_one_cnt = minus_one_cnt + 1
                else:
                    y[i] = 0
                    zero_cnt = zero_cnt + 1

            np.savetxt(f"./data/Y/y_k_{k}_hour_{hour}.csv", y)

        print(zero_cnt, one_cnt, minus_one_cnt)
        zero_cnt = 0
        one_cnt = 0
        minus_one_cnt = 0


if __name__ == "__main__":
    calculate_y(0.000002)
