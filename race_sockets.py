import asyncio
import json
import base64
from datetime import datetime
import websockets
from solders.pubkey import Pubkey  # type: ignore
from construct import Struct, Padding, Int64ul, Flag, Bytes
import grpc
import base58
from grpc import aio
from dotenv import load_dotenv
import backoff
from generated.geyser_pb2 import (
    SubscribeRequest,
    SubscribeRequestFilterTransactions,
    CommitmentLevel,
)
from generated.geyser_pb2_grpc import GeyserStub

# === Constants ===
RPC_PROVIDERS = [
    "wss://api.mainnet-beta.solana.com"
]
WSS_PUMPPORTAL = "wss://pumpportal.fun/api/data"
GRPC_ENDPOINT = "grpc.solanavibestation.com:10000"  # Add your endpoint

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
    "virtualTokenReserves" / Int64ul
)

# === Load leaderboard addresses ===
with open("leaderboard/weekly_leaderboard.json", encoding="utf-8") as f:
    leaderboard = json.load(f)
ADDRESSES = {kol["address"] for kol in leaderboard}

def format_trade(parsed_data, txn_sig):
    return {
        "mint": str(Pubkey.from_bytes(bytes(parsed_data.mint))),
        "sol_amount": parsed_data.solAmount / 10**9,
        "token_amount": parsed_data.tokenAmount / 10**6,
        "is_buy": parsed_data.isBuy,
        "user": str(Pubkey.from_bytes(bytes(parsed_data.user))),
        "timestamp": parsed_data.timestamp,
        "txn_sig": txn_sig,
    }

# === QuickNode consumer (copied from your working pattern) ===
async def public_feed(queue: asyncio.Queue, rpc_url: str, name: str, connect_delay: float = 0):
    await asyncio.sleep(connect_delay)  # Stagger connection attempts
    retry_delays = [5, 10, 20, 40, 80, 100]
    retry_idx = 0
    while True:  # Reconnection loop
        try:
            async with websockets.connect(rpc_url, open_timeout=8) as ws:
                retry_idx = 0  # Reset retry on successful connect
                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]},
                        {"commitment": "processed"}
                    ]
                }
                await ws.send(json.dumps(request))
                print(f"Subscribed to QuickNode logs on {name}...")
                while True:
                    try:
                        response = await ws.recv()
                        log_data = json.loads(response)
                        result_value = log_data.get("params", {}).get("result", {}).get("value", {})
                        txn_sig = result_value.get("signature", "")
                        logs = result_value.get("logs", [])
                    except Exception as e:
                        print(f"{name} error receiving message: {e}")
                        break

                    if "Program log: Instruction: Buy" in logs and "Program log: Instruction: Sell" not in logs:
                        for log_entry in logs:
                            if "Program data: vdt/" in log_entry:
                                try:
                                    program_data_base64 = log_entry.split("Program data: ")[1]
                                    program_data_bytes = base64.b64decode(program_data_base64)
                                    parsed_data = trade.parse(program_data_bytes)
                                    trade_data = format_trade(parsed_data, txn_sig)
                                except Exception as e:
                                    print(f"{name} error parsing trade: {e}")
                                    continue

                                if trade_data and trade_data["user"] in ADDRESSES:
                                    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                                    await queue.put((name, txn_sig, ts))
        except Exception as e:
            print(f"{name} connection error: {e}")
            pass
        delay = retry_delays[min(retry_idx, len(retry_delays) - 1)]
        print(f"{name} retrying in {delay}s...")
        await asyncio.sleep(delay)
        if retry_idx < len(retry_delays) - 1:
            retry_idx += 1

# === PumpPortal consumer ===
async def pumpportal_feed(queue: asyncio.Queue):
    await asyncio.sleep(1)
    
    while True:
        try:
            async with websockets.connect(WSS_PUMPPORTAL, open_timeout=8) as ws:
                payload = {"method": "subscribeAccountTrade", "keys": list(ADDRESSES)}
                await ws.send(json.dumps(payload))
                print("Subscribed to PumpPortal...")
                
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)  # Increased timeout to 30 seconds
                        data = json.loads(msg)
                        
                        if data.get("txType") == "buy" and data.get("traderPublicKey") in ADDRESSES:
                            sig = data.get("signature")
                            if sig:
                                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                                await queue.put(("pumpportal", sig, ts))
                    except asyncio.TimeoutError:
                        # Instead of breaking, just try to receive again
                        continue
                    except Exception as e:
                        print(f"PumpPortal error receiving message: {e}")
                        break
        except Exception as e:
            print(f"PumpPortal connection error: {e}")
        await asyncio.sleep(3)

# Modify grpc_feed to track ping times
async def grpc_feed(queue: asyncio.Queue):
    await asyncio.sleep(0.5)
    retry_delays = [2, 5, 10, 20, 40, 60]  # More aggressive retry strategy
    retry_idx = 0

    while True:
        channel = None
        try:
            # Optimize channel settings for lower latency
            channel = aio.insecure_channel(
                GRPC_ENDPOINT,
                options=[
                    ('grpc.keepalive_time_ms', 5000),             # More frequent keepalives
                    ('grpc.keepalive_timeout_ms', 1000),          # Shorter timeout
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0),
                    ('grpc.max_receive_message_length', 10 * 1024 * 1024),  # Smaller buffer
                    ('grpc.min_reconnect_backoff_ms', 100),       # Fast reconnect
                    ('grpc.max_reconnect_backoff_ms', 1000),      # Cap reconnect time
                    ('grpc.use_local_subchannel_pool', True),     # Use local subchannel pool
                    ('grpc.enable_retries', 1),                   # Enable retries
                    ('grpc.service_config', json.dumps({          # Configure retry policy
                        "methodConfig": [{
                            "name": [{}],
                            "retryPolicy": {
                                "maxAttempts": 5,
                                "initialBackoff": "0.1s",
                                "maxBackoff": "1s",
                                "backoffMultiplier": 1.5,
                                "retryableStatusCodes": ["UNAVAILABLE"]
                            }
                        }]
                    })),
                ]
            )
            stub = GeyserStub(channel)

            # Optimize subscription request for speed
            request = SubscribeRequest()
            request.commitment = CommitmentLevel.PROCESSED
            tx_filter = SubscribeRequestFilterTransactions()
            pumpfun_program = "4bcFeLv4f7wSWrL5gG1pA9F6vYQ2f7Yk6bHp7F6w8kUa"
            
            # Add program ID first for more efficient filtering
            tx_filter.account_include.append(pumpfun_program)
            
            # Pre-convert addresses to proper format if needed to avoid conversion during processing
            tx_filter.account_include.extend(ADDRESSES)
            request.transactions["pumpfun"].CopyFrom(tx_filter)

            print("Subscribed to gRPC feed with optimized settings...")
            
            # Use a faster processing approach
            stream = stub.Subscribe(iter([request]))

            async for update in stream:
                if not update.HasField('transaction'):
                    continue
                
                try:
                    # Direct access to signature for faster processing
                    sig_bytes = update.transaction.transaction.signature
                    sig = base58.b58encode(sig_bytes).decode() if not isinstance(sig_bytes, str) else sig_bytes
                    
                    # Quick check for buy transaction in logs before other processing
                    logs = update.transaction.transaction.meta.log_messages
                    if not any("Program log: Instruction: Buy" in log for log in logs):
                        continue
                    if any("Program log: Instruction: Sell" in log for log in logs):
                        continue
                    
                    # Get fee payer efficiently
                    fee_payer_bytes = update.transaction.transaction.transaction.message.account_keys[0]
                    fee_payer = base58.b58encode(fee_payer_bytes).decode() if not isinstance(fee_payer_bytes, str) else fee_payer_bytes

                    # Only process if it's from a tracked wallet
                    if fee_payer in ADDRESSES:
                        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        await queue.put(("grpc", sig, ts))

                except Exception as e:
                    continue

        except grpc.aio.AioRpcError as e:
            print(f"gRPC connection error: {e.code()} - {e.details()}")
            # More specific error handling based on error type
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print("gRPC server unavailable, attempting quick reconnect...")
                await asyncio.sleep(1)  # Shorter sleep for unavailable errors
            else:
                delay = retry_delays[min(retry_idx, len(retry_delays) - 1)]
                print(f"gRPC retrying in {delay}s...")
                await asyncio.sleep(delay)
                if retry_idx < len(retry_delays) - 1:
                    retry_idx += 1
        except Exception as e:
            print(f"gRPC unexpected error: {e}")
            delay = retry_delays[min(retry_idx, len(retry_delays) - 1)]
            print(f"gRPC retrying in {delay}s...")
            await asyncio.sleep(delay)
            if retry_idx < len(retry_delays) - 1:
                retry_idx += 1
        finally:
            if channel:
                try:
                    await channel.close()
                except:
                    pass

# === Race harness ===
async def race():
    queue = asyncio.Queue()
    tasks = [
        *[asyncio.create_task(public_feed(queue, rpc_url, f"rpc_{i+1}", connect_delay=i*1.5)) 
          for i, rpc_url in enumerate(RPC_PROVIDERS)],
        asyncio.create_task(pumpportal_feed(queue)),
        asyncio.create_task(grpc_feed(queue))
    ]

    seen = {}  # sig -> (source, timestamp as datetime)

    while True:
        source, sig, ts = await queue.get()
        now = datetime.now()
        if sig not in seen:
            print(f"[FASTEST] [{ts}] {source.upper()} got tx: {sig[:5]}")
            seen[sig] = (source, now)
        else:
            print(f"[{ts}] {source.upper()} got tx: {sig[:5]}")

if __name__ == "__main__":
    asyncio.run(race())
