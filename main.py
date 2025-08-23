#!/usr/bin/env python3
"""
Yellowstone gRPC Client for Python
A production-ready client with automatic reconnection and full error handling
"""

import json
import os
import sys
import asyncio
import logging
from typing import Optional, AsyncIterator
from datetime import datetime
import signal
import base58
import csv  # Add this import
from pathlib import Path  # Add this import
from io import StringIO
import queue

import grpc
from grpc import aio
from dotenv import load_dotenv
import backoff
from generated.geyser_pb2 import (
    SubscribeRequest,
    SubscribeRequestFilterSlots,
    SubscribeRequestPing,
    CommitmentLevel,
    SubscribeRequestFilterTransactions,  # <-- add this import
)
from generated.geyser_pb2_grpc import GeyserStub

# follow correct addresses
with open('leaderboard/weekly_leaderboard.json', encoding='utf-8') as f:
    top_kols = json.load(f)

addresses = [kol['address'] for kol in top_kols]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configure CSV buffering
csv_file = Path('pump_transactions.csv')
csv_buffer = StringIO()
csv_writer = csv.writer(csv_buffer)
BUFFER_FLUSH_SIZE = 10  # Flush every 10 transactions
tx_counter = 0

if not csv_file.exists():
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['signature', 'recv_wall_iso'])


class Config:
    """Configuration management"""
    def __init__(self):
        load_dotenv()
        self.endpoint = os.getenv('GEYSER_ENDPOINT', 'grpc.solanavibestation.com:443')
        self.x_token = os.getenv('GEYSER_ACCESS_TOKEN', '')
        
        # Validate configuration
        if not self.endpoint:
            raise ValueError("GEYSER_ENDPOINT is required")
        
        # Parse endpoint to ensure it has port
        if ':' not in self.endpoint:
            self.endpoint += ':443'  # Default to secure port
            
        logger.info(f"Configuration loaded - Endpoint: {self.endpoint}")


class TritonAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """Authentication plugin for x-token"""
    def __init__(self, x_token: str):
        self.x_token = x_token

    def __call__(self, context, callback):
        if self.x_token:
            callback((("x-token", self.x_token),), None)
        else:
            callback((), None)


class MessageHandler:
    """Handles different message types from the stream"""
    
    async def handle_message(self, update, stub) -> bool:
        """
        Process a single update message
        Returns True to continue, False to break the loop
        """
        try:
            update_type = update.WhichOneof('update_oneof')
            
            if update_type == 'transaction':
                global tx_counter, csv_buffer, csv_writer
                
                # Quick check for tracked wallets first
                try:
                    txn = update.transaction.transaction.transaction
                    if not (hasattr(txn, "message") and txn.message.account_keys):
                        return True
                    
                    fee_payer_bytes = txn.message.account_keys[0]
                    fee_payer = base58.b58encode(fee_payer_bytes).decode() if not isinstance(fee_payer_bytes, str) else fee_payer_bytes
                    
                    if fee_payer not in addresses:
                        return True
                except:
                    return True

                # Get signature and log
                try:
                    sig_bytes = update.transaction.transaction.signature
                    sig_b58 = base58.b58encode(sig_bytes).decode() if not isinstance(sig_bytes, str) else sig_bytes
                    current_time = datetime.utcnow().isoformat() + "Z"
                    
                    # Write to buffer
                    csv_writer.writerow([sig_b58, current_time])
                    tx_counter += 1
                    
                    # Flush buffer if needed
                    if tx_counter >= BUFFER_FLUSH_SIZE:
                        with open(csv_file, 'a', newline='') as f:
                            f.write(csv_buffer.getvalue())
                        csv_buffer.truncate(0)
                        csv_buffer.seek(0)
                        tx_counter = 0
                        
                except Exception as e:
                    logger.error(f"Failed to log transaction: {e}")

                # Minimal print output
                print(f"TX: {sig_b58} | Wallet: {fee_payer}")
                return True
                
            elif update_type == 'ping':
                return True
                
            elif update_type == 'pong':
                return True
                
            return True
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return False


class GrpcClient:
    """Manages gRPC connection"""
    
    def __init__(self, config: Config):
        self.config = config
        self.channel = None
        self.stub = None
    
    async def connect(self) -> 'GeyserStub':
        """Establish connection to gRPC server"""
        logger.info(f"Connecting to gRPC endpoint: {self.config.endpoint}")
        
        # Determine if we should use SSL or insecure channel
        host, port = self.config.endpoint.rsplit(':', 1)
        use_ssl = port == "443"

        if use_ssl:
            # Setup SSL credentials
            ssl_creds = grpc.ssl_channel_credentials()
            # Setup authentication if token provided
            if self.config.x_token:
                auth_creds = grpc.metadata_call_credentials(
                    TritonAuthMetadataPlugin(self.config.x_token)
                )
                credentials = grpc.composite_channel_credentials(ssl_creds, auth_creds)
            else:
                credentials = ssl_creds

            options = [
                ('grpc.keepalive_time_ms', 10000),  # More frequent keepalive
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
                ('grpc.http2.min_time_between_pings_ms', 10000),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
                ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
                ('grpc.enable_retries', 1),
                ('grpc.service_config', '{"loadBalancingConfig":[{"round_robin":{}}]}'),
            ]
            logger.info("Using secure (SSL/TLS) channel")
            self.channel = aio.secure_channel(
                self.config.endpoint,
                credentials,
                options=options
            )
        else:
            # Insecure channel for non-SSL endpoints
            options = [
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 10000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
            ]
            logger.info("Using insecure (plaintext) channel")
            self.channel = aio.insecure_channel(
                self.config.endpoint,
                options=options
            )

        self.stub = GeyserStub(self.channel)
        
        logger.info("Successfully connected to Yellowstone gRPC")
        return self.stub
    
    async def close(self):
        """Close the gRPC channel"""
        if self.channel:
            await self.channel.close()


class SubscriptionManager:
    """Manages subscription and message streaming"""
    
    def __init__(self, client: GrpcClient, shutdown_event: asyncio.Event):
        self.client = client
        self.handler = MessageHandler()
        self.shutdown_event = shutdown_event
        self.ping_queue = asyncio.Queue(maxsize=1)  # Limit ping queue size
    
    async def run(self, stub: 'GeyserStub'):
        """Run the subscription loop"""
        # Create subscription request - optimized for transactions only
        request = SubscribeRequest()
        request.commitment = CommitmentLevel.PROCESSED  # Use PROCESSED instead of CONFIRMED for speed

        # Setup transaction filter for Pump.fun program and addresses
        tx_filter = SubscribeRequestFilterTransactions()
        pumpfun_program = "4bcFeLv4f7wSWrL5gG1pA9F6vYQ2f7Yk6bHp7F6w8kUa"
        tx_filter.account_include.append(pumpfun_program)
        tx_filter.account_include.extend(addresses)
        request.transactions["pumpfun"].CopyFrom(tx_filter)
        
        # Removed slot filter setup since we don't need it

        # Create request iterator that handles pings
        async def request_iterator():
            # First, yield the initial subscription request
            yield request
            
            # Then handle ping responses
            while not self.shutdown_event.is_set():
                try:
                    # Wait for ping with timeout to check shutdown periodically
                    ping_id = await asyncio.wait_for(
                        self.ping_queue.get(),
                        timeout=1.0
                    )
                    
                    # Send pong response
                    pong_request = SubscribeRequest()
                    ping = SubscribeRequestPing()
                    ping.id = ping_id
                    pong_request.ping.CopyFrom(ping)
                    yield pong_request
                    
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in request iterator: {e}")
                    break
        
        logger.info("Subscribed to slot updates, waiting for messages...")
        
        try:
            # Start the subscription
            stream = stub.Subscribe(request_iterator())
            
            # Process messages
            async for update in stream:
                if self.shutdown_event.is_set():
                    break
                    
                # Handle ping specially to queue pong response
                if update.HasField('ping'):
                    try:
                        ping_id = update.ping.id if hasattr(update.ping, 'id') else 1
                        await self.ping_queue.put(ping_id)
                        logger.info(f"Received ping from server (id={ping_id}) - replying to keep connection alive")
                    except Exception as e:
                        logger.error(f"Error handling ping: {e}")
                    continue
                
                # Handle other messages
                should_continue = await self.handler.handle_message(update, stub)
                if not should_continue:
                    break
                    
        except grpc.RpcError as e:
            if not self.shutdown_event.is_set():
                logger.error(f"Stream error: {e.code()} - {e.details()}")
                raise
        except asyncio.CancelledError:
            # Suppress during shutdown
            if not self.shutdown_event.is_set():
                raise
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"Unexpected error: {e}")
                raise
        finally:
            logger.info("Stream closed")


async def run_with_reconnect(config: Config, shutdown_event: asyncio.Event):
    """Main loop with reconnection logic"""
    
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=None,  # Retry forever
        max_time=None,   # No maximum time
        on_backoff=lambda details: logger.warning(
            f"Connection failed, will retry in {details['wait']:.1f}s... "
            f"(attempt {details['tries']})"
        )
    )
    async def connect_and_subscribe():
        """Connect and run subscription with retry logic"""
        if shutdown_event.is_set():
            return
            
        client = GrpcClient(config)
        subscription_manager = SubscriptionManager(client, shutdown_event)
        
        try:
            # Connect to server
            stub = await client.connect()
            
            # Run subscription
            await subscription_manager.run(stub)
            
            # Check if we're shutting down
            if shutdown_event.is_set():
                return
                
            # If we get here, stream ended normally
            logger.warning("Stream ended, will reconnect...")
            
            # Small delay before reconnecting
            await asyncio.sleep(1)
            
            # Always raise to trigger reconnection
            raise Exception("Stream closed, triggering reconnection")
            
        finally:
            # Clean up
            await client.close()
    
    # Run with reconnection
    try:
        await connect_and_subscribe()
    except asyncio.CancelledError:
        # Suppress cancelled error during shutdown
        pass


async def main():
    """Main entry point"""
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.info("Shutting down gracefully...")
        shutdown_event.set()

    try:
        # Load configuration
        config = Config()
        
        # Cross-platform signal handling
        if os.name == 'posix':  # Unix/Linux/macOS
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, signal_handler)
        else:  # Windows
            signal.signal(signal.SIGINT, lambda s, f: signal_handler())
            signal.signal(signal.SIGTERM, lambda s, f: signal_handler())
        
        # Run with reconnection logic
        await run_with_reconnect(config, shutdown_event)
        
    except KeyboardInterrupt:
        # Handle Ctrl+C on Windows
        signal_handler()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Suppress KeyboardInterrupt traceback
        pass