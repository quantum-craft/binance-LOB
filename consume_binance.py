import asyncio
import websockets
import json
import requests
from datetime import datetime
import base64
import os
import subprocess

lob_uri = "wss://fstream.binance.com/ws/btcusdt@depth@0ms"
trade_uri = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
request = "https://www.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"


class StreamHandler:

    def __init__(self, hours=24):

        self.hours = hours

        batch_time = datetime.now().strftime("%Y%m%d-%H%M")
        self.write_path = "[OPEN]_{}_f_BTCUSDT_events.bin".format(batch_time)

        file = open(self.write_path, "wb")  # initialise file
        file.close()

    def encode_event(self, payload):

        encoded = str(payload)
        ascii_encoded = encoded.encode("ascii")
        byte_encoded = base64.b64encode(ascii_encoded)

        return byte_encoded

    async def get_orders(self):

        while True:

            try:

                async with websockets.connect(lob_uri) as websocket:

                    while True:

                        depth_update = await websocket.recv()
                        depth_update = json.loads(depth_update)
                        depth_update_byte = self.encode_event(depth_update)

                        with open(self.write_path, "ab") as f:
                            f.write(depth_update_byte)
                            f.write(b"\n")
                            f.close()

            except Exception as e:
                print(e)

    async def get_trades(self):
        while True:
            try:
                async with websockets.connect(trade_uri) as websocket:
                    while True:
                        trade_update = await websocket.recv()
                        trade_update = json.loads(trade_update)
                        trade_update_byte = self.encode_event(trade_update)

                        with open(self.write_path, "ab") as f:
                            f.write(trade_update_byte)
                            f.write(b"\n")
                            f.close()

            except Exception as e:
                print(e)

    async def time_batch(self):

        while True:

            await asyncio.sleep(5)  # warmup

            snapshot = requests.get(request)
            snapshot = json.loads(snapshot.content.decode("utf-8"))
            snapshot["e"] = "orderbookSnapshot"
            snapshot_byte = self.encode_event(snapshot)

            with open(self.write_path, "ab") as f:
                f.write(snapshot_byte)
                f.write(b"\n")
                f.close()

            await asyncio.sleep(60 * 60 * self.hours - 5)

            # create new write path
            batch_time = datetime.now().strftime("%Y%m%d-%H%M")
            new_write_path = "[OPEN]_{}_f_BTCUSDT_events.bin".format(batch_time)

            file = open(new_write_path, "wb")  # initialise new file
            file.close()

            last_write_path = self.write_path
            self.write_path = new_write_path

            # rename last write path to notify that it's ready to be transfered
            os.rename(last_write_path, "[RDY]_" + last_write_path[7:])

    async def move_to_gcs(self):

        while True:

            await asyncio.sleep(60 * 60 * self.hours + 10)  # hours + 10 sec time delta

            for file in os.listdir("/root/binance-stream"):

                if "RDY" in file:

                    zip_name = file[6:-4] + ".zip"
                    subprocess.run(["zip", zip_name, file])
                    subprocess.run(["gsutil", "cp", zip_name, "gs://xxxxxxxxxxxxxx"])
                    subprocess.run(["rm", zip_name, file])

    async def main(self):

        while True:

            await asyncio.gather(
                self.get_trades(),
                self.get_orders(),
                self.time_batch(),
                self.move_to_gcs(),
            )


if __name__ == "__main__":

    stream = StreamHandler()
    asyncio.get_event_loop().run_until_complete(stream.main())
