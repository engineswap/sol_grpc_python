import asyncio
import json
import base64
from datetime import datetime
import websockets
from solders.pubkey import Pubkey  # type: ignore
from construct import Struct, Padding, Int64ul, Flag, Bytes

# --- gRPC imports ---
import grpc
from grpc import aio
from generated.geyser_pb2 import (
    SubscribeRequest,
    SubscribeRequestFilterTransactions,
    CommitmentLevel,
    SubscribeRequestPing,
)
from generated.geyser_pb2_grpc import GeyserStub

# === Constants ===
WSS_PUMPPORTAL = "wss://pumpportal.fun/api/data"
SVS_GRPC_ENDPOINT = "grpc.solanavibestation.com:10000"  # SVS endpoint
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# === Struct ===
trade = Struct(
    Padding(8),
    "mint" / Bytes(32),
    "solAmount" / Int64ul,
    "tokenAmount" / Int64ul,
    "isBuy" / Flag,
    "user" / Bytes(32),
    "timestamp" / Int64ul,
    "virtualSolReserves" / Int64ul,
    "virtualTokenReserves" / Int64ul,
)

# === Load leaderboard addresses ===
with open("leaderboard/weekly_leaderboard.json", encoding="utf-8") as f:
    leaderboard = json.load(f)
ADDRESSES = {kol["address"] for kol in leaderboard}

def format_trade(parsed_data, txn_sig: str):
    return {
        "mint": str(Pubkey.from_bytes(bytes(parsed_data.mint))),
        "sol_amount": parsed_data.solAmount / 10**9,
        "token_amount": parsed_data.tokenAmount / 10**6,
        "is_buy": parsed_data.isBuy,
        "user": str(Pubkey.from_bytes(bytes(parsed_data.user))),
        "timestamp": parsed_data.timestamp,
        "txn_sig": txn_sig,
    }

# ---------- PumpPortal consumer ----------
async def pumpportal_feed(queue: asyncio.Queue):
    await asyncio.sleep(0.5)
    while True:
        try:
            async with websockets.connect(WSS_PUMPPORTAL, open_timeout=8) as ws:
                payload = {"method": "subscribeAccountTrade", "keys": list(ADDRESSES)}
                await ws.send(json.dumps(payload))
                print("Subscribed to PumpPortal...")
                while True:
                    try:
                        msg = await ws.recv()
                        data = json.loads(msg)
                    except Exception as e:
                        print(f"PumpPortal error receiving message: {e}")
                        break

                    if data.get("txType") == "buy" and data.get("traderPublicKey") in ADDRESSES:
                        sig = data.get("signature")
                        if sig:
                            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                            await queue.put(("pumpportal", sig, ts))
        except Exception as e:
            print(f"PumpPortal connection error: {e}")
        await asyncio.sleep(3)

# ---------- Helper: build tx filter that works across geyser forks ----------
def build_tx_filter(pump_program: str) -> SubscribeRequestFilterTransactions:
    flt = SubscribeRequestFilterTransactions()

    # vote is almost always present
    try:
        flt.vote = False
    except Exception:
        pass

    # some forks expose "failed", others don't
    try:
        flt.failed = False
    except Exception:
        pass

    # common: either "programs" or "mentions" exists
    attached = False
    try:
        # repeated field; use extend to avoid replacement errors
        flt.programs.extend([pump_program])
        attached = True
    except Exception:
        pass

    if not attached:
        try:
            flt.mentions.extend([pump_program])
            attached = True
        except Exception:
            pass

    if not attached:
        raise RuntimeError(
            "Your geyser build exposes neither 'programs' nor 'mentions' on "
            "SubscribeRequestFilterTransactions. Ask SVS which field to use."
        )

    return flt

# ---------- SVS gRPC consumer ----------
async def svs_grpc_feed(queue: asyncio.Queue, endpoint: str = SVS_GRPC_ENDPOINT):
    retry_delays = [3, 5, 10, 20, 40, 60]
    idx = 0
    while True:
        try:
            # Switch to secure_channel with creds if your endpoint is TLS
            async with aio.insecure_channel(endpoint) as channel:
                await channel.channel_ready()
                stub = GeyserStub(channel)

                tx_filter = build_tx_filter(PUMP_FUN_PROGRAM)
                req = SubscribeRequest(
                    commitment=CommitmentLevel.PROCESSED,
                    transactions=[tx_filter],
                    ping=SubscribeRequestPing(id="keepalive", interval=15),
                )

                print("Subscribed to SVS gRPC (transactions filtered to Pump.fun program)...")
                stream = stub.Subscribe(req)

                async for update in stream:
                    if not hasattr(update, "transaction") or update.transaction is None:
                        continue

                    txu = update.transaction
                    sig = getattr(txu, "signature", "") or ""
                    meta = getattr(txu, "meta", None)
                    logs = list(getattr(meta, "log_messages", [])) if meta else []

                    # Match Pump.fun buy-only like your WS logic
                    if any("Program log: Instruction: Buy" in l for l in logs) and not any(
                        "Program log: Instruction: Sell" in l for l in logs
                    ):
                        for log_entry in logs:
                            if "Program data: " in log_entry:
                                payload = log_entry.split("Program data: ", 1)[1]
                                # Sometimes prefixed as "vdt/..."
                                if payload.startswith("vdt/"):
                                    payload = payload[4:]
                                try:
                                    program_data_bytes = base64.b64decode(payload)
                                    parsed_data = trade.parse(program_data_bytes)
                                    t = format_trade(parsed_data, sig)
                                except Exception:
                                    continue

                                if t and t["is_buy"] and t["user"] in ADDRESSES:
                                    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                                    await queue.put(("svs_grpc", sig, ts))
        except Exception as e:
            delay = retry_delays[min(idx, len(retry_delays) - 1)]
            print(f"SVS gRPC connection/stream error: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            if idx < len(retry_delays) - 1:
                idx += 1

# ---------- Race harness (PumpPortal vs SVS gRPC) ----------
async def race():
    queue = asyncio.Queue()
    tasks = [
        asyncio.create_task(svs_grpc_feed(queue, SVS_GRPC_ENDPOINT)),
        asyncio.create_task(pumpportal_feed(queue)),
    ]

    seen = {}  # sig -> (source, first_ts)
    while True:
        source, sig, ts = await queue.get()
        if sig not in seen:
            print(f"[FASTEST] [{ts}] {source.upper()} got tx: {sig[:5]}")
            seen[sig] = (source, ts)
        else:
            print(f"[{ts}] {source.upper()} got tx: {sig[:5]}")

if __name__ == "__main__":
    try:
        asyncio.run(race())
    except KeyboardInterrupt:
        print("Shutting down...")
