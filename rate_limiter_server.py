#!/usr/bin/env python3
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

import socket
import threading
import argparse
import logging
import time
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

DEFAULT_RATE = 1.0       # tokens per second
DEFAULT_CAPACITY = 5.0   # burst capacity (tokens)
BUCKET_TTL_SECONDS = 600 # cleanup idle buckets after 10 minutes

class TokenBucket:
    __slots__ = ("rate", "capacity", "tokens", "last_ts", "lock", "last_used")

    def __init__(self, rate: float = DEFAULT_RATE, capacity: float = DEFAULT_CAPACITY):
        self.rate = float(rate)
        self.capacity = float(capacity)
        self.tokens = float(capacity)  # start full
        self.last_ts = time.monotonic()
        self.lock = threading.Lock()
        self.last_used = time.monotonic()

    def _refill(self, now: float):
        if now <= self.last_ts:
            return
        elapsed = now - self.last_ts
        added = elapsed * self.rate
        if added > 0:
            self.tokens = min(self.capacity, self.tokens + added)
            self.last_ts = now

    def allow_request(self, cost: float = 1.0) -> bool:
        now = time.monotonic()
        with self.lock:
            self._refill(now)
            self.last_used = now
            if self.tokens >= cost:
                self.tokens -= cost
                return True
            return False

    def to_dict(self):
        # snapshot view (acquire lock to be consistent)
        now = time.monotonic()
        with self.lock:
            self._refill(now)
            return {
                "rate": self.rate,
                "capacity": self.capacity,
                "tokens": self.tokens,
                "last_used_seconds_ago": now - self.last_used
            }

    def set_config(self, rate: float, capacity: float):
        now = time.monotonic()
        with self.lock:
            # refill before changing capacity to preserve fairness
            self._refill(now)
            self.rate = float(rate)
            self.capacity = float(capacity)
            # cap tokens to new capacity
            self.tokens = min(self.tokens, self.capacity)
            self.last_ts = now
            self.last_used = now

class RateLimiter:
    def __init__(self):
        self.buckets = {}  # client_id -> TokenBucket
        self.lock = threading.Lock()
        # start cleaner thread
        t = threading.Thread(target=self._cleanup_thread, daemon=True)
        t.start()

    def _get_or_create_bucket(self, client_id: str) -> TokenBucket:
        with self.lock:
            b = self.buckets.get(client_id)
            if b is None:
                b = TokenBucket()
                self.buckets[client_id] = b
            return b

    def allow(self, client_id: str) -> bool:
        b = self._get_or_create_bucket(client_id)
        return b.allow_request()

    def set(self, client_id: str, rate: float, capacity: float):
        with self.lock:
            b = self.buckets.get(client_id)
            if b is None:
                b = TokenBucket(rate=rate, capacity=capacity)
                self.buckets[client_id] = b
            else:
                b.set_config(rate, capacity)

    def get(self, client_id: str):
        with self.lock:
            b = self.buckets.get(client_id)
        if b is None:
            return None
        return b.to_dict()

    def _cleanup_thread(self):
        while True:
            now = time.monotonic()
            with self.lock:
                to_del = [cid for cid, b in self.buckets.items() if now - b.last_used > BUCKET_TTL_SECONDS]
                for cid in to_del:
                    logging.info("Cleaning up idle bucket %s", cid)
                    del self.buckets[cid]
            time.sleep(60)

# TCP server
class TLSSimulatedError(Exception):
    pass

class RateLimiterServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 9010):
        self.host = host
        self.port = port
        self.sock = None
        self.rl = RateLimiter()
        self.stop_event = threading.Event()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(200)
        logging.info("RateLimiterServer listening on %s:%d", self.host, self.port)
        try:
            while not self.stop_event.is_set():
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Shutting down server...")
        finally:
            self.stop()

    def stop(self):
        self.stop_event.set()
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        logging.info("Server stopped.")

    def _handle_client(self, conn: socket.socket, addr):
        logging.info("Client connected %s", addr)
        try:
            f = conn.makefile("rwb")
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.decode().strip()
                if not line:
                    continue
                parts = line.split()
                cmd = parts[0].upper()
                if cmd == "ALLOW" and len(parts) == 2:
                    client_id = parts[1]
                    ok = self.rl.allow(client_id)
                    resp = "ALLOW\n" if ok else "DENY\n"
                    conn.sendall(resp.encode())
                elif cmd == "SET" and len(parts) == 4:
                    client_id = parts[1]
                    try:
                        rate = float(parts[2])
                        cap = float(parts[3])
                        if rate <= 0 or cap <= 0:
                            raise ValueError("non-positive")
                    except ValueError:
                        conn.sendall(b"ERROR invalid rate/capacity\n")
                        continue
                    self.rl.set(client_id, rate, cap)
                    conn.sendall(b"OK\n")
                elif cmd == "GET" and len(parts) == 2:
                    client_id = parts[1]
                    info = self.rl.get(client_id)
                    if info is None:
                        conn.sendall(b"NOTFOUND\n")
                    else:
                        conn.sendall((json.dumps(info) + "\n").encode())
                elif cmd == "PING":
                    conn.sendall(b"PONG\n")
                else:
                    conn.sendall(b"ERROR unknown command\n")
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            logging.info("Client disconnected %s", addr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9010)
    args = ap.parse_args()
    srv = RateLimiterServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()
