#!/usr/bin/env python3
"""
circuit_breaker_server.py

Circuit Breaker Service over TCP.

Protocol:
    CALL <service_id> - Attempt to call service
        -> returns "SUCCESS\n", "FAILURE\n", or "CIRCUIT_OPEN\n"
    
    STATUS <service_id> - Get circuit breaker status
        -> returns JSON status
    
    CONFIG <service_id> <failure_threshold> <timeout_ms> <success_threshold>
        -> returns "OK\n" or "ERROR\n"
"""

import socket
import threading
import argparse
import logging
import time
import json
from enum import Enum

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Circuit is open, rejecting requests
    HALF_OPEN = "HALF_OPEN"  # Testing if service has recovered

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout_seconds=60, success_threshold=3):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.success_threshold = success_threshold
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = threading.Lock()

    def call(self, success=True):
        with self.lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.timeout_seconds:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    return "CIRCUIT_OPEN"
            
            if self.state == CircuitState.HALF_OPEN:
                if success:
                    self.success_count += 1
                    if self.success_count >= self.success_threshold:
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0
                    return "SUCCESS"
                else:
                    self.state = CircuitState.OPEN
                    self.last_failure_time = time.time()
                    return "FAILURE"
            
            if self.state == CircuitState.CLOSED:
                if success:
                    self.failure_count = 0
                    return "SUCCESS"
                else:
                    self.failure_count += 1
                    if self.failure_count >= self.failure_threshold:
                        self.state = CircuitState.OPEN
                        self.last_failure_time = time.time()
                    return "FAILURE"

    def get_status(self):
        with self.lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "failure_threshold": self.failure_threshold,
                "timeout_seconds": self.timeout_seconds,
                "last_failure_time": self.last_failure_time
            }

class CircuitBreakerServer:
    def __init__(self, host="0.0.0.0", port=9011):
        self.host = host
        self.port = port
        self.breakers = {}
        self.lock = threading.Lock()
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("CircuitBreakerServer listening on %s:%d", self.host, self.port)
        
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
                
                if cmd == "CALL" and len(parts) == 2:
                    service_id = parts[1]
                    # Simulate success/failure (in real implementation, this would call the actual service)
                    import random
                    success = random.random() > 0.3  # 70% success rate
                    
                    breaker = self._get_breaker(service_id)
                    result = breaker.call(success)
                    conn.sendall(f"{result}\n".encode())
                
                elif cmd == "STATUS" and len(parts) == 2:
                    service_id = parts[1]
                    breaker = self._get_breaker(service_id)
                    status = breaker.get_status()
                    conn.sendall((json.dumps(status) + "\n").encode())
                
                elif cmd == "CONFIG" and len(parts) == 5:
                    service_id = parts[1]
                    try:
                        failure_threshold = int(parts[2])
                        timeout_seconds = int(parts[3])
                        success_threshold = int(parts[4])
                        
                        with self.lock:
                            self.breakers[service_id] = CircuitBreaker(
                                failure_threshold, timeout_seconds, success_threshold
                            )
                        conn.sendall(b"OK\n")
                    except ValueError:
                        conn.sendall(b"ERROR invalid parameters\n")
                
                else:
                    conn.sendall(b"ERROR unknown command\n")
                    
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            conn.close()
            logging.info("Client disconnected %s", addr)

    def _get_breaker(self, service_id):
        with self.lock:
            if service_id not in self.breakers:
                self.breakers[service_id] = CircuitBreaker()
            return self.breakers[service_id]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9011)
    args = ap.parse_args()
    
    srv = CircuitBreakerServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()