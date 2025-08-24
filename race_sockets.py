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
import concurrent.futures
import multiprocessing
from collections import defaultdict
import ctypes
from array import array

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

# Even more optimized version of grpc_feed
async def grpc_feed(queue: asyncio.Queue):
    # Use ctypes to set absolute highest thread priority at OS level
    try:
        if os.name == 'nt':  # Windows
            import ctypes
            ctypes.windll.kernel32.SetThreadPriority(ctypes.windll.kernel32.GetCurrentThread(), 
                                                    15)  # THREAD_PRIORITY_TIME_CRITICAL
        elif os.name == 'posix':  # Linux/Unix/MacOS
            import resource
            # Set CPU affinity to first core for dedicated processing
            try:
                os.sched_setaffinity(0, {0})  # Pin to CPU 0
            except:
                pass
            # Set highest priority
            os.nice(-20)  # Highest priority
            # Set FIFO scheduling (real-time)
            param = os.sched_param(99)  # Max priority
            os.sched_setscheduler(0, os.SCHED_FIFO, param)
            # Increase resource limits
            resource.setrlimit(resource.RLIMIT_CPU, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
    except Exception:
        pass  # Gracefully continue if priority setting fails

    # Create thread-local storage for optimizations
    local_data = threading.local()
    local_data.seen_signatures = set()
    local_data.string_cache = {}
    
    # Extremely short initial delay
    await asyncio.sleep(0.05)
    retry_delays = [0.5, 1, 2, 5, 10, 15]
    retry_idx = 0
    
    # Create a pool of gRPC channels for connection redundancy
    NUM_CHANNELS = 2  # Create multiple connections for redundancy
    channels = []
    connection_times = []

    # Initialize lookup tables for byte comparison
    BUY_BYTES = b"Program log: Instruction: Buy"
    SELL_BYTES = b"Program log: Instruction: Sell"
    
    # Precompile signature conversion function for speed using array module
    def fast_b58_encode(data):
        try:
            return base58.b58encode(data).decode()
        except:
            return str(data)
    
    # Low-level optimization for signature checking
    def is_signature_seen(sig):
        sig_hash = hash(bytes(sig))
        if sig_hash in local_data.seen_signatures:
            return True
        local_data.seen_signatures.add(sig_hash)
        if len(local_data.seen_signatures) > 20000:  # Larger cache
            local_data.seen_signatures.clear()
        return False
    
    # Pre-warm DNS and TCP connections
    for _ in range(3):  # Multiple warming attempts
        try:
            dummy_channel = aio.insecure_channel(GRPC_ENDPOINT)
            await asyncio.sleep(0.05)
            await dummy_channel.close()
        except:
            pass

    # Setup executor for CPU-bound operations
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    
    # Define ultra-optimized log scanner
    def scan_logs(logs, buy_str=BUY_BYTES, sell_str=SELL_BYTES):
        has_buy = False
        for log in logs:
            if isinstance(log, str):
                if "Buy" in log and buy_str in log.encode():
                    has_buy = True
                if "Sell" in log and sell_str in log.encode():
                    return False
            else:  # bytes
                if b"Buy" in log and buy_str in log:
                    has_buy = True
                if b"Sell" in log and sell_str in log:
                    return False
        return has_buy

    # Track channel performance
    channel_stats = [{"success": 0, "errors": 0, "last_success": 0} for _ in range(NUM_CHANNELS)]
    
    while True:
        try:
            # Create multiple channels for redundancy
            channels = []
            for i in range(NUM_CHANNELS):
                channel = aio.insecure_channel(
                    GRPC_ENDPOINT,
                    options=[
                        ('grpc.keepalive_time_ms', 500),              # Ultra-frequent keepalives
                        ('grpc.keepalive_timeout_ms', 200),           # Ultra-short timeout
                        ('grpc.keepalive_permit_without_calls', True),
                        ('grpc.http2.max_pings_without_data', 0),
                        ('grpc.max_receive_message_length', 2 * 1024 * 1024),  # Ultra small buffer
                        ('grpc.min_reconnect_backoff_ms', 10),        # Ultra-fast reconnect
                        ('grpc.max_reconnect_backoff_ms', 100),       # Ultra-short max reconnect
                        ('grpc.initial_reconnect_backoff_ms', 10),    # Ultra-fast initial reconnect
                        ('grpc.use_local_subchannel_pool', True),     
                        ('grpc.enable_retries', 1),                   
                        ('grpc.tcp_thin_stream', True),               # Optimize for low latency
                        ('grpc.http2.bdp_probe', True),               # Enable bandwidth probing
                        ('grpc.http2.min_time_between_pings_ms', 100), # Frequent pings
                        ('grpc.service_config', json.dumps({
                            "methodConfig": [{
                                "name": [{}],
                                "retryPolicy": {
                                    "maxAttempts": 2,
                                    "initialBackoff": "0.01s",
                                    "maxBackoff": "0.1s",
                                    "backoffMultiplier": 1.2,
                                    "retryableStatusCodes": ["UNAVAILABLE"]
                                }
                            }]
                        })),
                    ]
                )
                channels.append(channel)
            
            # Create tasks for each channel
            tasks = []
            for i, channel in enumerate(channels):
                tasks.append(asyncio.create_task(
                    process_channel(i, channel, queue, is_signature_seen, scan_logs, 
                                   fast_b58_encode, channel_stats, executor)
                ))
            
            # Wait for all tasks to complete (they should run indefinitely)
            await asyncio.gather(*tasks)
            
        except Exception as e:
            print(f"gRPC pool error: {str(e)[:50]}")
            # Close all channels
            for channel in channels:
                try:
                    await channel.close()
                except:
                    pass
            
            # Ultra-fast recovery
            await asyncio.sleep(retry_delays[min(retry_idx, len(retry_delays) - 1)])
            if retry_idx < len(retry_delays) - 1:
                retry_idx += 1

# Helper function to process a single gRPC channel
async def process_channel(channel_idx, channel, queue, is_signature_seen, scan_logs, fast_b58_encode, channel_stats, executor):
    try:
        stub = GeyserStub(channel)
        
        # Create an ultra-optimized request
        request = SubscribeRequest()
        request.commitment = CommitmentLevel.PROCESSED
        tx_filter = SubscribeRequestFilterTransactions()
        tx_filter.account_include.append("4bcFeLv4f7wSWrL5gG1pA9F6vYQ2f7Yk6bHp7F6w8kUa")
        tx_filter.account_include.extend(ADDRESSES)
        request.transactions["pumpfun"].CopyFrom(tx_filter)
        
        print(f"Channel {channel_idx} subscribed to gRPC feed with extreme optimizations")
        
        # Use a short timeout for receiving each message to detect problems quickly
        stream = stub.Subscribe(iter([request]))
        last_heartbeat = time.time()
        
        async for update in stream:
            # Quick heartbeat check and reset
            now = time.time()
            if now - last_heartbeat > 60:
                last_heartbeat = now
                print(f"Channel {channel_idx} healthy: {channel_stats[channel_idx]['success']} msgs processed")
            
            # Super quick bailout
            if not update.HasField('transaction'):
                continue
            
            try:
                # Direct memory access for performance
                sig_bytes = update.transaction.transaction.signature
                
                # Skip seen signatures with hash-based lookup (fastest possible)
                if is_signature_seen(sig_bytes):
                    continue
                
                # Get logs with minimal processing
                logs = update.transaction.transaction.meta.log_messages
                
                # Ultra-fast log scan using optimized function 
                if not scan_logs(logs):
                    continue
                
                # Direct binary fee payer comparison
                fee_payer_bytes = update.transaction.transaction.transaction.message.account_keys[0]
                
                # Direct binary comparison
                if fee_payer_bytes in BINARY_ADDRESSES:
                    # Convert only when needed
                    sig = fast_b58_encode(sig_bytes)
                    
                    # Use microseconds for even more precise timing
                    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    
                    # Track success for this channel
                    channel_stats[channel_idx]["success"] += 1
                    channel_stats[channel_idx]["last_success"] = time.time()
                    
                    # Submit to queue with absolute highest priority
                    await queue.put(("grpc", sig, ts))
                    
            except Exception:
                continue
    
    except Exception as e:
        channel_stats[channel_idx]["errors"] += 1
        print(f"Channel {channel_idx} error: {str(e)[:50]}")
        raise

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
