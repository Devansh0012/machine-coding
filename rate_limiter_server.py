#!/usr/bin/env python3  # Shebang to run with Python 3
"""
rate_limiter_server.py

Simple Token-Bucket Rate Limiter Service over TCP.

Protocol (line-based):
    ALLOW <client_id>
        -> returns "ALLOW\n" or "DENY\n"

    SET <client_id> <rate_per_sec> <capacity>
        -> returns "OK\n" or "ERROR ...\n"

    GET <client_id>
        -> returns JSON status line (single-line)

    PING
        -> returns "PONG\n"

Defaults:
    rate = 1 token/sec
    capacity = 5 tokens

Test with: nc 127.0.0.1 9010
"""

import socket      # For TCP socket operations
import threading   # For multi-threaded client handling
import argparse    # For command-line argument parsing
import logging     # For logging server events
import time        # For time-based token bucket calculations
import json        # For JSON serialization of bucket status

# Configure logging with timestamp, level, thread name, and message
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

DEFAULT_RATE = 1.0       # Default tokens generated per second
DEFAULT_CAPACITY = 5.0   # Default maximum tokens in bucket
BUCKET_TTL_SECONDS = 600 # Time after which idle buckets are cleaned up

class TokenBucket:
        # Use __slots__ to optimize memory usage and prevent dynamic attribute creation
        __slots__ = ("rate", "capacity", "tokens", "last_ts", "lock", "last_used")

        def __init__(self, rate: float = DEFAULT_RATE, capacity: float = DEFAULT_CAPACITY):
                self.rate = float(rate)              # Tokens per second generation rate
                self.capacity = float(capacity)      # Maximum tokens the bucket can hold
                self.tokens = float(capacity)        # Current tokens (start with full bucket)
                self.last_ts = time.monotonic()      # Last time tokens were refilled
                self.lock = threading.Lock()         # Thread-safe access to bucket state
                self.last_used = time.monotonic()    # Last time bucket was accessed

        def _refill(self, now: float):
                if now <= self.last_ts:              # If time hasn't advanced, no refill needed
                        return
                elapsed = now - self.last_ts         # Calculate time elapsed since last refill
                added = elapsed * self.rate          # Calculate tokens to add based on rate
                if added > 0:                        # Only update if tokens were added
                        self.tokens = min(self.capacity, self.tokens + added)  # Cap at capacity
                        self.last_ts = now               # Update last refill timestamp

        def allow_request(self, cost: float = 1.0) -> bool:
                now = time.monotonic()               # Get current monotonic time
                with self.lock:                      # Acquire lock for thread safety
                        self._refill(now)                # Refill tokens based on elapsed time
                        self.last_used = now             # Update last used timestamp
                        if self.tokens >= cost:          # Check if enough tokens available
                                self.tokens -= cost          # Consume tokens for this request
                                return True                  # Allow the request
                        return False                     # Deny the request (insufficient tokens)

        def to_dict(self):
                # Create a snapshot dictionary of bucket state for status queries
                now = time.monotonic()               # Get current time
                with self.lock:                      # Acquire lock for consistent snapshot
                        self._refill(now)                # Ensure tokens are up-to-date
                        return {
                                "rate": self.rate,                                    # Tokens per second
                                "capacity": self.capacity,                            # Maximum capacity
                                "tokens": self.tokens,                                # Current tokens
                                "last_used_seconds_ago": now - self.last_used        # Seconds since last use
                        }

        def set_config(self, rate: float, capacity: float):
                now = time.monotonic()               # Get current time
                with self.lock:                      # Acquire lock for thread safety
                        self._refill(now)                # Refill before changing config for fairness
                        self.rate = float(rate)          # Update token generation rate
                        self.capacity = float(capacity)  # Update maximum capacity
                        self.tokens = min(self.tokens, self.capacity)  # Cap current tokens to new capacity
                        self.last_ts = now               # Update refill timestamp
                        self.last_used = now             # Update last used timestamp

class RateLimiter:
        def __init__(self):
                self.buckets = {}                    # Dictionary mapping client_id to TokenBucket
                self.lock = threading.Lock()         # Lock for thread-safe bucket management
                # Start background cleanup thread as daemon (dies when main program exits)
                t = threading.Thread(target=self._cleanup_thread, daemon=True)
                t.start()

        def _get_or_create_bucket(self, client_id: str) -> TokenBucket:
                with self.lock:                      # Acquire lock for thread-safe access
                        b = self.buckets.get(client_id)  # Try to get existing bucket
                        if b is None:                    # If bucket doesn't exist
                                b = TokenBucket()            # Create new bucket with default settings
                                self.buckets[client_id] = b  # Store bucket in dictionary
                        return b                         # Return the bucket

        def allow(self, client_id: str) -> bool:
                b = self._get_or_create_bucket(client_id)  # Get or create bucket for client
                return b.allow_request()                   # Check if request is allowed

        def set(self, client_id: str, rate: float, capacity: float):
                with self.lock:                      # Acquire lock for thread safety
                        b = self.buckets.get(client_id)  # Try to get existing bucket
                        if b is None:                    # If bucket doesn't exist
                                b = TokenBucket(rate=rate, capacity=capacity)  # Create with specified config
                                self.buckets[client_id] = b  # Store new bucket
                        else:
                                b.set_config(rate, capacity) # Update existing bucket configuration

        def get(self, client_id: str):
                with self.lock:                      # Acquire lock for thread safety
                        b = self.buckets.get(client_id)  # Try to get bucket
                if b is None:                        # If bucket doesn't exist
                        return None                      # Return None
                return b.to_dict()                   # Return bucket status as dictionary

        def _cleanup_thread(self):
                while True:                          # Infinite cleanup loop
                        now = time.monotonic()           # Get current time
                        with self.lock:                  # Acquire lock for thread safety
                                # Find buckets that haven't been used recently
                                to_del = [cid for cid, b in self.buckets.items() if now - b.last_used > BUCKET_TTL_SECONDS]
                                for cid in to_del:           # Delete idle buckets
                                        logging.info("Cleaning up idle bucket %s", cid)  # Log cleanup
                                        del self.buckets[cid]    # Remove bucket from dictionary
                        time.sleep(60)                   # Sleep for 60 seconds before next cleanup

# TCP server implementation
class TLSSimulatedError(Exception):
        pass                                     # Custom exception class (unused in current code)

class RateLimiterServer:
        def __init__(self, host: str = "0.0.0.0", port: int = 9010):
                self.host = host                     # Server bind address
                self.port = port                     # Server bind port
                self.sock = None                     # Server socket (initialized later)
                self.rl = RateLimiter()              # Rate limiter instance
                self.stop_event = threading.Event()  # Event to signal server shutdown

        def start(self):
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow address reuse
                self.sock.bind((self.host, self.port))        # Bind socket to address and port
                self.sock.listen(200)                         # Listen for connections (queue size 200)
                logging.info("RateLimiterServer listening on %s:%d", self.host, self.port)  # Log startup
                try:
                        while not self.stop_event.is_set():       # Main server loop
                                conn, addr = self.sock.accept()        # Accept new client connection
                                # Start new thread to handle client (daemon thread)
                                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
                except KeyboardInterrupt:                     # Handle Ctrl+C gracefully
                        logging.info("Shutting down server...")
                finally:
                        self.stop()                               # Ensure cleanup

        def stop(self):
                self.stop_event.set()                # Signal all threads to stop
                try:
                        if self.sock:                    # If socket exists
                                self.sock.close()            # Close the server socket
                except Exception:
                        pass                             # Ignore errors during cleanup
                logging.info("Server stopped.")     # Log shutdown

        def _handle_client(self, conn: socket.socket, addr):
                logging.info("Client connected %s", addr)    # Log client connection
                try:
                        f = conn.makefile("rwb")                  # Create file-like object for socket
                        while True:                               # Client message loop
                                line = f.readline()                   # Read line from client
                                if not line:                          # If no data (client disconnected)
                                        break
                                line = line.decode().strip()          # Decode bytes to string and strip whitespace
                                if not line:                          # Skip empty lines
                                        continue
                                parts = line.split()                  # Split command into parts
                                cmd = parts[0].upper()                # Get command (case insensitive)
                                
                                if cmd == "ALLOW" and len(parts) == 2:        # ALLOW command with client_id
                                        client_id = parts[1]                      # Extract client ID
                                        ok = self.rl.allow(client_id)             # Check if request allowed
                                        resp = "ALLOW\n" if ok else "DENY\n"      # Prepare response
                                        conn.sendall(resp.encode())               # Send response to client
                                        
                                elif cmd == "SET" and len(parts) == 4:        # SET command with parameters
                                        client_id = parts[1]                      # Extract client ID
                                        try:
                                                rate = float(parts[2])                # Parse rate parameter
                                                cap = float(parts[3])                 # Parse capacity parameter
                                                if rate <= 0 or cap <= 0:            # Validate positive values
                                                        raise ValueError("non-positive")
                                        except ValueError:                        # Handle invalid parameters
                                                conn.sendall(b"ERROR invalid rate/capacity\n")
                                                continue
                                        self.rl.set(client_id, rate, cap)         # Update rate limiter config
                                        conn.sendall(b"OK\n")                     # Send success response
                                        
                                elif cmd == "GET" and len(parts) == 2:        # GET command for status
                                        client_id = parts[1]                      # Extract client ID
                                        info = self.rl.get(client_id)             # Get bucket status
                                        if info is None:                          # If bucket doesn't exist
                                                conn.sendall(b"NOTFOUND\n")           # Send not found response
                                        else:
                                                conn.sendall((json.dumps(info) + "\n").encode())  # Send JSON status
                                                
                                elif cmd == "PING":                           # PING command for health check
                                        conn.sendall(b"PONG\n")                   # Send PONG response
                                        
                                else:                                         # Unknown command
                                        conn.sendall(b"ERROR unknown command\n")  # Send error response
                                        
                except Exception:                             # Handle any exceptions
                        logging.exception("Error handling client %s", addr)  # Log exception details
                finally:
                        try:
                                conn.close()                          # Close client connection
                        except Exception:
                                pass                                  # Ignore errors during cleanup
                        logging.info("Client disconnected %s", addr)     # Log client disconnection

def main():
        ap = argparse.ArgumentParser()           # Create argument parser
        ap.add_argument("--host", default="0.0.0.0")    # Add host argument with default
        ap.add_argument("--port", type=int, default=9010)  # Add port argument with default
        args = ap.parse_args()                   # Parse command line arguments
        srv = RateLimiterServer(args.host, args.port)    # Create server instance
        srv.start()                              # Start the server

if __name__ == "__main__":                   # If script is run directly
        main()                                   # Call main function

"""
CODE SUMMARY:
=============

This is a complete TCP-based rate limiting server implementation using the token bucket algorithm.

Key Components:

1. TokenBucket Class:
     - Implements the token bucket rate limiting algorithm
     - Tokens are generated at a configurable rate and stored up to a maximum capacity
     - Requests consume tokens; if insufficient tokens are available, requests are denied
     - Thread-safe with proper locking mechanisms
     - Automatically refills tokens based on elapsed time

2. RateLimiter Class:
     - Manages multiple TokenBucket instances, one per client ID
     - Provides methods to check allowance, set configurations, and get status
     - Includes background cleanup thread to remove idle buckets after 10 minutes
     - Thread-safe bucket creation and management

3. RateLimiterServer Class:
     - TCP server that listens for client connections
     - Handles multiple clients concurrently using threading
     - Implements a simple text-based protocol with commands:
         * ALLOW <client_id> - Check if request is allowed
         * SET <client_id> <rate> <capacity> - Configure rate limits
         * GET <client_id> - Get current bucket status as JSON
         * PING - Health check command
     - Graceful shutdown handling with proper resource cleanup

4. Protocol Features:
     - Line-based text protocol for easy testing with tools like netcat
     - JSON responses for status queries
     - Error handling for invalid commands and parameters
     - Connection management with proper client disconnect handling

The server is designed to be production-ready with proper logging, error handling, 
resource cleanup, and concurrent client support. It can be used to rate limit 
API requests, web traffic, or any other scenario requiring request throttling.

Usage: Run the server and connect with 'nc 127.0.0.1 9010' to test commands.
"""
