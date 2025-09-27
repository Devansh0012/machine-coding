#!/usr/bin/env python3
"""
distributed_lock_server.py

Distributed Lock Service over TCP.

Protocol:
    ACQUIRE <lock_name> <ttl_seconds> <client_id> - Acquire a lock
        -> returns "ACQUIRED\n" or "DENIED\n"
    
    RELEASE <lock_name> <client_id> - Release a lock
        -> returns "RELEASED\n" or "NOT_OWNER\n"
    
    STATUS <lock_name> - Get lock status
        -> returns JSON status or "NOT_FOUND\n"
    
    HEARTBEAT <lock_name> <client_id> - Extend lock TTL
        -> returns "EXTENDED\n" or "NOT_OWNER\n"
"""

import socket
import threading
import argparse
import logging
import time
import json
from dataclasses import dataclass
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

@dataclass
class Lock:
    name: str
    owner: str
    acquired_at: float
    expires_at: float
    ttl_seconds: int

class DistributedLockService:
    def __init__(self):
        self.locks = {}  # lock_name -> Lock
        self.lock = threading.Lock()
        # Start cleanup thread
        threading.Thread(target=self._cleanup_expired_locks, daemon=True).start()

    def acquire_lock(self, lock_name: str, client_id: str, ttl_seconds: int) -> bool:
        now = time.time()
        with self.lock:
            # Check if lock exists and is not expired
            if lock_name in self.locks:
                existing_lock = self.locks[lock_name]
                if existing_lock.expires_at > now:
                    # Lock is still valid and owned by someone else
                    if existing_lock.owner != client_id:
                        return False
                    # Same client trying to re-acquire, extend TTL
                    existing_lock.expires_at = now + ttl_seconds
                    existing_lock.ttl_seconds = ttl_seconds
                    return True
            
            # Acquire the lock
            self.locks[lock_name] = Lock(
                name=lock_name,
                owner=client_id,
                acquired_at=now,
                expires_at=now + ttl_seconds,
                ttl_seconds=ttl_seconds
            )
            logging.info("Lock '%s' acquired by client '%s' for %ds", lock_name, client_id, ttl_seconds)
            return True

    def release_lock(self, lock_name: str, client_id: str) -> bool:
        with self.lock:
            if lock_name not in self.locks:
                return False
            
            lock_obj = self.locks[lock_name]
            if lock_obj.owner != client_id:
                return False
            
            del self.locks[lock_name]
            logging.info("Lock '%s' released by client '%s'", lock_name, client_id)
            return True

    def heartbeat(self, lock_name: str, client_id: str, ttl_seconds: int) -> bool:
        now = time.time()
        with self.lock:
            if lock_name not in self.locks:
                return False
            
            lock_obj = self.locks[lock_name]
            if lock_obj.owner != client_id or lock_obj.expires_at <= now:
                return False
            
            # Extend TTL
            lock_obj.expires_at = now + ttl_seconds
            lock_obj.ttl_seconds = ttl_seconds
            return True

    def get_lock_status(self, lock_name: str) -> Optional[dict]:
        now = time.time()
        with self.lock:
            if lock_name not in self.locks:
                return None
            
            lock_obj = self.locks[lock_name]
            if lock_obj.expires_at <= now:
                del self.locks[lock_name]
                return None
            
            return {
                "name": lock_obj.name,
                "owner": lock_obj.owner,
                "acquired_at": lock_obj.acquired_at,
                "expires_at": lock_obj.expires_at,
                "ttl_seconds": lock_obj.ttl_seconds,
                "remaining_seconds": lock_obj.expires_at - now
            }

    def _cleanup_expired_locks(self):
        while True:
            now = time.time()
            with self.lock:
                expired_locks = [name for name, lock_obj in self.locks.items() 
                               if lock_obj.expires_at <= now]
                for lock_name in expired_locks:
                    logging.info("Lock '%s' expired, cleaning up", lock_name)
                    del self.locks[lock_name]
            
            time.sleep(10)  # Cleanup every 10 seconds

class DistributedLockServer:
    def __init__(self, host="0.0.0.0", port=9012):
        self.host = host
        self.port = port
        self.lock_service = DistributedLockService()
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("DistributedLockServer listening on %s:%d", self.host, self.port)
        
        try:
            while True:
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            if self.sock:
                self.sock.close()

    def _handle_client(self, conn, addr):
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
                
                if cmd == "ACQUIRE" and len(parts) == 4:
                    lock_name, ttl_str, client_id = parts[1], parts[2], parts[3]
                    try:
                        ttl_seconds = int(ttl_str)
                        if ttl_seconds <= 0:
                            raise ValueError("TTL must be positive")
                        
                        success = self.lock_service.acquire_lock(lock_name, client_id, ttl_seconds)
                        response = "ACQUIRED\n" if success else "DENIED\n"
                        conn.sendall(response.encode())
                    except ValueError:
                        conn.sendall(b"ERROR invalid TTL\n")
                
                elif cmd == "RELEASE" and len(parts) == 3:
                    lock_name, client_id = parts[1], parts[2]
                    success = self.lock_service.release_lock(lock_name, client_id)
                    response = "RELEASED\n" if success else "NOT_OWNER\n"
                    conn.sendall(response.encode())
                
                elif cmd == "STATUS" and len(parts) == 2:
                    lock_name = parts[1]
                    status = self.lock_service.get_lock_status(lock_name)
                    if status:
                        conn.sendall((json.dumps(status) + "\n").encode())
                    else:
                        conn.sendall(b"NOT_FOUND\n")
                
                elif cmd == "HEARTBEAT" and len(parts) == 3:
                    lock_name, client_id = parts[1], parts[2]
                    # Use default TTL extension of 30 seconds
                    success = self.lock_service.heartbeat(lock_name, client_id, 30)
                    response = "EXTENDED\n" if success else "NOT_OWNER\n"
                    conn.sendall(response.encode())
                
                else:
                    conn.sendall(b"ERROR unknown command\n")
                    
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            conn.close()
            logging.info("Client disconnected %s", addr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9012)
    args = ap.parse_args()
    
    srv = DistributedLockServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()