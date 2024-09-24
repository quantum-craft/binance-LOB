import numpy as np
from main import partial_orderbook_generator
from sklearn.preprocessing import StandardScaler
from main import get_all_data_blocks
from datetime import timedelta
from datetime import datetime


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
