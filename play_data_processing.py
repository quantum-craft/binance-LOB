import numpy as np
from main import partial_orderbook_generator
from sklearn.preprocessing import StandardScaler
from main import get_all_data_blocks
from datetime import timedelta
from datetime import datetime


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
