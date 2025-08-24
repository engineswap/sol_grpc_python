import asyncio
import json
import csv
import os
import websockets
from time import time_ns

# === Load leaderboard addresses ===
with open("leaderboard/weekly_leaderboard.json", encoding="utf-8") as f:
    leaderboard = json.load(f)
ADDRESSES = {kol["address"] for kol in leaderboard}

CSV_FILE = "pumpportal_txns.csv"

def append_csv(timestamp_ms: int, sig: str):
    write_header = not os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if write_header:
            writer.writerow(["timestamp_ms", "txn_id"])
        writer.writerow([timestamp_ms, sig])

async def subscribe():
    uri = "wss://pumpportal.fun/api/data"
    async with websockets.connect(uri, ping_interval=20, ping_timeout=10, close_timeout=5) as websocket:
        print(f"Connected to {uri}")

        payload = {"method": "subscribeAccountTrade", "keys": list(ADDRESSES)}
        await websocket.send(json.dumps(payload))

        async for message in websocket:
            try:
                msg = json.loads(message)
                sig = msg.get("signature")
                if sig:
                    ts_ms = time_ns() // 1_000_000
                    print(f"[{ts_ms}] {sig}")
                    append_csv(ts_ms, sig)
            except Exception:
                continue

if __name__ == "__main__":
    asyncio.run(subscribe())
