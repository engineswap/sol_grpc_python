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
import threading
import os
import time
from collections import defaultdict

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

# Prepare address data structures for ultra-fast lookups
BINARY_ADDRESSES = set()
ADDRESS_LOOKUP = {}
try:
    for address in ADDRESSES:
        # Convert to binary format for fast comparisons
        binary_addr = base58.b58decode(address)
        BINARY_ADDRESSES.add(binary_addr)
        ADDRESS_LOOKUP[binary_addr] = address
except Exception as e:
    print(f"Error pre-processing addresses: {e}")

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

# Modify grpc_feed to be even faster
async def grpc_feed(queue: asyncio.Queue):
    # Set highest thread priority for this coroutine if possible
    try:
        thread_id = threading.get_native_id()
        if hasattr(os, 'sched_setscheduler'):
            os.sched_setscheduler(0, os.SCHED_FIFO, os.sched_param(99))
        elif hasattr(os, 'setpriority'):
            os.setpriority(os.PRIO_PROCESS, thread_id, -20)  # Lowest nice value = highest priority
    except Exception:
        pass  # Ignore if not supported
        
    await asyncio.sleep(0.1)  # Shorter initial delay
    retry_delays = [1, 2, 5, 10, 20, 30]  # Even more aggressive retry strategy
    retry_idx = 0
    seen_signatures = set()  # Local cache to avoid reprocessing

    # Warm up DNS and network connection
    try:
        asyncio.create_task(aio.insecure_channel(GRPC_ENDPOINT).close())
        await asyncio.sleep(0.1)
    except:
        pass

    while True:
        channel = None
        try:
            # Even more optimized channel settings
            channel = aio.insecure_channel(
                GRPC_ENDPOINT,
                options=[
                    ('grpc.keepalive_time_ms', 1000),             # Very frequent keepalives
                    ('grpc.keepalive_timeout_ms', 500),           # Very short timeout
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0),
                    ('grpc.max_receive_message_length', 5 * 1024 * 1024),  # Even smaller buffer
                    ('grpc.min_reconnect_backoff_ms', 50),        # Very fast reconnect
                    ('grpc.max_reconnect_backoff_ms', 500),       # Very short max reconnect
                    ('grpc.initial_reconnect_backoff_ms', 50),    # Fast initial reconnect
                    ('grpc.use_local_subchannel_pool', True),     
                    ('grpc.enable_retries', 1),                   
                    ('grpc.client_idle_timeout_ms', 60000),       # Disconnect after 1min idle
                    ('grpc.max_send_message_length', 1 * 1024 * 1024),  # Smaller send buffer
                    ('grpc.service_config', json.dumps({
                        "methodConfig": [{
                            "name": [{}],
                            "retryPolicy": {
                                "maxAttempts": 3,
                                "initialBackoff": "0.05s",
                                "maxBackoff": "0.5s",
                                "backoffMultiplier": 1.5,
                                "retryableStatusCodes": ["UNAVAILABLE"]
                            }
                        }]
                    })),
                ]
            )
            stub = GeyserStub(channel)

            # Create an optimized request
            request = SubscribeRequest()
            request.commitment = CommitmentLevel.PROCESSED
            tx_filter = SubscribeRequestFilterTransactions()
            pumpfun_program = "4bcFeLv4f7wSWrL5gG1pA9F6vYQ2f7Yk6bHp7F6w8kUa"
            
            tx_filter.account_include.append(pumpfun_program)
            tx_filter.account_include.extend(ADDRESSES)
            request.transactions["pumpfun"].CopyFrom(tx_filter)

            # Prepare common strings to avoid string operations during processing
            BUY_INSTRUCTION = "Program log: Instruction: Buy"
            SELL_INSTRUCTION = "Program log: Instruction: Sell"
            
            print("Subscribed to gRPC feed with ultra-optimized settings...")
            
            stream = stub.Subscribe(iter([request]))
            start_time = time.time()

            async for update in stream:
                # Quick bailout if not a transaction
                if not update.HasField('transaction'):
                    continue
                
                try:
                    # Direct binary access for maximum performance
                    sig_bytes = update.transaction.transaction.signature
                    
                    # Skip if we've seen this signature already (local cache)
                    if sig_bytes in seen_signatures:
                        continue
                        
                    # Get logs first to quickly filter out non-buy transactions
                    logs = update.transaction.transaction.meta.log_messages
                    
                    # Ultra-fast log scanning with short-circuit evaluation
                    has_buy = False
                    has_sell = False
                    for log in logs:
                        if BUY_INSTRUCTION in log:
                            has_buy = True
                        if SELL_INSTRUCTION in log:
                            has_sell = True
                            break
                    
                    if not has_buy or has_sell:
                        continue
                    
                    # Get fee payer directly in binary form for fastest comparison
                    fee_payer_bytes = update.transaction.transaction.transaction.message.account_keys[0]
                    
                    # Direct binary comparison if possible
                    if fee_payer_bytes in BINARY_ADDRESSES:
                        # Convert signature to string only once we know we need it
                        sig = base58.b58encode(sig_bytes).decode() if not isinstance(sig_bytes, str) else sig_bytes
                        
                        # Add to seen cache to avoid reprocessing
                        seen_signatures.add(sig_bytes)
                        if len(seen_signatures) > 10000:  # Prevent unlimited growth
                            seen_signatures.clear()
                            
                        # Get timestamp with minimal formatting
                        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        await queue.put(("grpc", sig, ts))
                        
                        # Calculate and periodically log latency metrics
                        if time.time() - start_time > 60:  # Log every minute
                            start_time = time.time()
                            print("gRPC connection healthy and optimized")

                except Exception as e:
                    continue

        except grpc.aio.AioRpcError as e:
            code = e.code()
            if code == grpc.StatusCode.UNAVAILABLE:
                print("gRPC quick reconnect...")
                await asyncio.sleep(0.5)
            elif code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                print("gRPC resource exhausted, backing off...")
                await asyncio.sleep(retry_delays[min(retry_idx, len(retry_delays) - 1)])
                retry_idx = min(retry_idx + 1, len(retry_delays) - 1)
            else:
                print(f"gRPC error: {code}")
                await asyncio.sleep(retry_delays[min(retry_idx, len(retry_delays) - 1)])
                retry_idx = min(retry_idx + 1, len(retry_delays) - 1)
        except Exception as e:
            print(f"gRPC error: {str(e)[:100]}")
            await asyncio.sleep(retry_delays[min(retry_idx, len(retry_delays) - 1)])
            retry_idx = min(retry_idx + 1, len(retry_delays) - 1)
        finally:
            if channel:
                try:
                    await channel.close()
                except:
                    pass
            # Quick recovery after errors
            await asyncio.sleep(0.1)

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
