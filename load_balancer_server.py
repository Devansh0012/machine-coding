#!/usr/bin/env python3
"""
load_balancer_server.py

Load Balancer Service over TCP.

Protocol:
    ADD_BACKEND <backend_id> <host> <port> - Add backend server
    REMOVE_BACKEND <backend_id> - Remove backend server
    SET_ALGORITHM <round_robin|least_connections|weighted> - Set load balancing algorithm
    GET_STATS - Get load balancer statistics
    FORWARD <data> - Forward request to backend (returns response)
"""

import socket
import threading
import argparse
import logging
import time
import json
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class Algorithm(Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"

@dataclass
class Backend:
    backend_id: str
    host: str
    port: int
    healthy: bool = True
    connections: int = 0
    weight: int = 1
    total_requests: int = 0
    failed_requests: int = 0
    last_health_check: float = 0

class LoadBalancer:
    def __init__(self):
        self.backends: Dict[str, Backend] = {}
        self.algorithm = Algorithm.ROUND_ROBIN
        self.round_robin_index = 0
        self.lock = threading.Lock()
        
        # Start health checker
        threading.Thread(target=self._health_checker, daemon=True).start()

    def add_backend(self, backend_id: str, host: str, port: int, weight: int = 1):
        with self.lock:
            self.backends[backend_id] = Backend(backend_id, host, port, weight=weight)
            logging.info("Added backend %s (%s:%d)", backend_id, host, port)

    def remove_backend(self, backend_id: str):
        with self.lock:
            if backend_id in self.backends:
                del self.backends[backend_id]
                logging.info("Removed backend %s", backend_id)
                return True
            return False

    def set_algorithm(self, algorithm: Algorithm):
        with self.lock:
            self.algorithm = algorithm
            logging.info("Set load balancing algorithm to %s", algorithm.value)

    def get_backend(self) -> Optional[Backend]:
        with self.lock:
            healthy_backends = [b for b in self.backends.values() if b.healthy]
            if not healthy_backends:
                return None

            if self.algorithm == Algorithm.ROUND_ROBIN:
                backend = healthy_backends[self.round_robin_index % len(healthy_backends)]
                self.round_robin_index += 1
                return backend
            
            elif self.algorithm == Algorithm.LEAST_CONNECTIONS:
                return min(healthy_backends, key=lambda b: b.connections)
            
            elif self.algorithm == Algorithm.WEIGHTED:
                # Weighted random selection
                weights = [b.weight for b in healthy_backends]
                return random.choices(healthy_backends, weights=weights)[0]
            
            return healthy_backends[0]

    def forward_request(self, data: str) -> str:
        backend = self.get_backend()
        if not backend:
            return "ERROR: No healthy backends available"
        
        # Simulate request forwarding
        with self.lock:
            backend.connections += 1
            backend.total_requests += 1
        
        try:
            # In real implementation, this would forward to actual backend
            # For simulation, we'll just echo with backend info
            response = f"Response from {backend.backend_id}: {data}"
            time.sleep(0.1)  # Simulate processing time
            return response
        except Exception as e:
            with self.lock:
                backend.failed_requests += 1
            return f"ERROR: Backend {backend.backend_id} failed: {str(e)}"
        finally:
            with self.lock:
                backend.connections -= 1

    def get_stats(self) -> dict:
        with self.lock:
            stats = {
                "algorithm": self.algorithm.value,
                "total_backends": len(self.backends),
                "healthy_backends": len([b for b in self.backends.values() if b.healthy]),
                "backends": {}
            }
            
            for backend in self.backends.values():
                stats["backends"][backend.backend_id] = {
                    "host": backend.host,
                    "port": backend.port,
                    "healthy": backend.healthy,
                    "connections": backend.connections,
                    "weight": backend.weight,
                    "total_requests": backend.total_requests,
                    "failed_requests": backend.failed_requests,
                    "success_rate": (backend.total_requests - backend.failed_requests) / max(backend.total_requests, 1)
                }
            
            return stats

    def _health_checker(self):
        while True:
            with self.lock:
                backends_to_check = list(self.backends.values())
            
            for backend in backends_to_check:
                try:
                    # Simulate health check (in real implementation, would ping the backend)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(2)
                    result = sock.connect_ex((backend.host, backend.port))
                    sock.close()
                    
                    healthy = (result == 0)
                    
                    if backend.healthy != healthy:
                        logging.info("Backend %s health changed: %s -> %s", 
                                   backend.backend_id, backend.healthy, healthy)
                    
                    backend.healthy = healthy
                    backend.last_health_check = time.time()
                    
                except Exception as e:
                    logging.warning("Health check failed for %s: %s", backend.backend_id, e)
                    backend.healthy = False
                    backend.last_health_check = time.time()
            
            time.sleep(5)  # Health check every 5 seconds

class LoadBalancerServer:
    def __init__(self, host="0.0.0.0", port=9013):
        self.host = host
        self.port = port
        self.lb = LoadBalancer()
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("LoadBalancerServer listening on %s:%d", self.host, self.port)
        
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
                
                parts = line.split(maxsplit=4)
                cmd = parts[0].upper()
                
                if cmd == "ADD_BACKEND" and len(parts) >= 4:
                    backend_id, host, port_str = parts[1], parts[2], parts[3]
                    weight = int(parts[4]) if len(parts) > 4 else 1
                    try:
                        port = int(port_str)
                        self.lb.add_backend(backend_id, host, port, weight)
                        conn.sendall(b"OK\n")
                    except ValueError:
                        conn.sendall(b"ERROR invalid port\n")
                
                elif cmd == "REMOVE_BACKEND" and len(parts) == 2:
                    backend_id = parts[1]
                    success = self.lb.remove_backend(backend_id)
                    response = "OK\n" if success else "NOT_FOUND\n"
                    conn.sendall(response.encode())
                
                elif cmd == "SET_ALGORITHM" and len(parts) == 2:
                    algorithm_str = parts[1].lower()
                    try:
                        algorithm = Algorithm(algorithm_str)
                        self.lb.set_algorithm(algorithm)
                        conn.sendall(b"OK\n")
                    except ValueError:
                        conn.sendall(b"ERROR invalid algorithm\n")
                
                elif cmd == "GET_STATS":
                    stats = self.lb.get_stats()
                    conn.sendall((json.dumps(stats) + "\n").encode())
                
                elif cmd == "FORWARD" and len(parts) >= 2:
                    data = " ".join(parts[1:])
                    response = self.lb.forward_request(data)
                    conn.sendall((response + "\n").encode())
                
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
    ap.add_argument("--port", type=int, default=9013)
    args = ap.parse_args()
    
    srv = LoadBalancerServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()