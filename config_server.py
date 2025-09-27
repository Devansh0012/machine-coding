#!/usr/bin/env python3
"""
config_server.py

Configuration Service over TCP.

Protocol:
    SET <key> <value> - Set configuration value
    GET <key> - Get configuration value  
    GET_VERSION <key> <version> - Get specific version
    DELETE <key> - Delete configuration
    LIST - List all configuration keys
    SUBSCRIBE <pattern> - Subscribe to configuration changes
    HISTORY <key> - Get version history for key
"""

import socket
import threading
import argparse
import logging
import time
import json
import fnmatch
from dataclasses import dataclass, asdict
from typing import Dict, List, Set, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

@dataclass
class ConfigEntry:
    key: str
    value: str
    version: int
    timestamp: float
    created_by: str = "system"

class ConfigService:
    def __init__(self):
        self.configs: Dict[str, List[ConfigEntry]] = {}  # key -> [versions]
        self.current_versions: Dict[str, int] = {}  # key -> current_version
        self.subscribers: Set[socket.socket] = set()
        self.subscription_patterns: Dict[socket.socket, List[str]] = {}
        self.lock = threading.Lock()

    def set_config(self, key: str, value: str, client_info: str = "unknown") -> ConfigEntry:
        now = time.time()
        
        with self.lock:
            if key not in self.configs:
                self.configs[key] = []
                self.current_versions[key] = 0
            
            # Increment version
            new_version = self.current_versions[key] + 1
            self.current_versions[key] = new_version
            
            # Create new entry
            entry = ConfigEntry(
                key=key,
                value=value,
                version=new_version,
                timestamp=now,
                created_by=client_info
            )
            
            # Add to history
            self.configs[key].append(entry)
            
            # Keep only last 10 versions
            if len(self.configs[key]) > 10:
                self.configs[key] = self.configs[key][-10:]
            
            logging.info("Config updated: %s = %s (v%d)", key, value, new_version)
        
        # Notify subscribers
        self._notify_subscribers(key, entry)
        return entry

    def get_config(self, key: str, version: Optional[int] = None) -> Optional[ConfigEntry]:
        with self.lock:
            if key not in self.configs:
                return None
            
            versions = self.configs[key]
            if not versions:
                return None
            
            if version is None:
                return versions[-1]  # Latest version
            
            # Find specific version
            for entry in versions:
                if entry.version == version:
                    return entry
            
            return None

    def delete_config(self, key: str) -> bool:
        with self.lock:
            if key not in self.configs:
                return False
            
            del self.configs[key]
            del self.current_versions[key]
            logging.info("Config deleted: %s", key)
        
        # Notify subscribers about deletion
        self._notify_subscribers(key, None)
        return True

    def list_configs(self) -> Dict[str, ConfigEntry]:
        with self.lock:
            result = {}
            for key, versions in self.configs.items():
                if versions:
                    result[key] = versions[-1]  # Latest version
            return result

    def get_history(self, key: str) -> List[ConfigEntry]:
        with self.lock:
            return self.configs.get(key, []).copy()

    def subscribe(self, conn: socket.socket, pattern: str):
        with self.lock:
            self.subscribers.add(conn)
            if conn not in self.subscription_patterns:
                self.subscription_patterns[conn] = []
            self.subscription_patterns[conn].append(pattern)
            logging.info("Client subscribed to pattern: %s", pattern)

    def unsubscribe(self, conn: socket.socket):
        with self.lock:
            self.subscribers.discard(conn)
            self.subscription_patterns.pop(conn, None)

    def _notify_subscribers(self, key: str, entry: Optional[ConfigEntry]):
        notification = {
            "type": "config_change" if entry else "config_delete",
            "key": key,
            "timestamp": time.time()
        }
        
        if entry:
            notification.update({
                "value": entry.value,
                "version": entry.version,
                "created_by": entry.created_by
            })
        
        message = json.dumps(notification) + "\n"
        
        with self.lock:
            dead_subscribers = []
            for conn in list(self.subscribers):
                # Check if this subscriber is interested in this key
                patterns = self.subscription_patterns.get(conn, [])
                should_notify = False
                
                for pattern in patterns:
                    if fnmatch.fnmatch(key, pattern):
                        should_notify = True
                        break
                
                if should_notify:
                    try:
                        conn.sendall(message.encode())
                    except Exception:
                        dead_subscribers.append(conn)
            
            # Remove dead subscribers
            for conn in dead_subscribers:
                self.unsubscribe(conn)

class ConfigServer:
    def __init__(self, host="0.0.0.0", port=9015):
        self.host = host
        self.port = port
        self.service = ConfigService()
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("ConfigServer listening on %s:%d", self.host, self.port)
        
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
                
                parts = line.split(maxsplit=2)
                cmd = parts[0].upper()
                
                if cmd == "SET" and len(parts) == 3:
                    key, value = parts[1], parts[2]
                    entry = self.service.set_config(key, value, f"client_{addr[0]}:{addr[1]}")
                    result = {
                        "status": "OK",
                        "version": entry.version,
                        "timestamp": entry.timestamp
                    }
                    conn.sendall((json.dumps(result) + "\n").encode())
                
                elif cmd == "GET" and len(parts) == 2:
                    key = parts[1]
                    entry = self.service.get_config(key)
                    if entry:
                        result = {
                            "key": entry.key,
                            "value": entry.value,
                            "version": entry.version,
                            "timestamp": entry.timestamp,
                            "created_by": entry.created_by
                        }
                        conn.sendall((json.dumps(result) + "\n").encode())
                    else:
                        conn.sendall(b'{"error": "Key not found"}\n')
                
                elif cmd == "GET_VERSION" and len(parts) == 3:
                    key = parts[1]
                    try:
                        version = int(parts[2])
                        entry = self.service.get_config(key, version)
                        if entry:
                            result = asdict(entry)
                            conn.sendall((json.dumps(result) + "\n").encode())
                        else:
                            conn.sendall(b'{"error": "Version not found"}\n')
                    except ValueError:
                        conn.sendall(b'{"error": "Invalid version"}\n')
                
                elif cmd == "DELETE" and len(parts) == 2:
                    key = parts[1]
                    success = self.service.delete_config(key)
                    result = {"status": "OK" if success else "NOT_FOUND"}
                    conn.sendall((json.dumps(result) + "\n").encode())
                
                elif cmd == "LIST":
                    configs = self.service.list_configs()
                    result = {}
                    for key, entry in configs.items():
                        result[key] = {
                            "value": entry.value,
                            "version": entry.version,
                            "timestamp": entry.timestamp
                        }
                    conn.sendall((json.dumps(result) + "\n").encode())
                
                elif cmd == "SUBSCRIBE" and len(parts) == 2:
                    pattern = parts[1]
                    self.service.subscribe(conn, pattern)
                    conn.sendall(b'{"status": "SUBSCRIBED"}\n')
                
                elif cmd == "HISTORY" and len(parts) == 2:
                    key = parts[1]
                    history = self.service.get_history(key)
                    result = [asdict(entry) for entry in history]
                    conn.sendall((json.dumps(result) + "\n").encode())
                
                else:
                    conn.sendall(b'{"error": "Unknown command"}\n')
                    
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            self.service.unsubscribe(conn)
            conn.close()
            logging.info("Client disconnected %s", addr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9015)
    args = ap.parse_args()
    
    srv = ConfigServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()