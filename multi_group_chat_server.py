#!/usr/bin/env python3
"""
multi_group_chat_server.py

TCP Group Chat Server with:
- Multiple groups
- History replay (last 15 min)
- Persistence to JSON
Test with: telnet 127.0.0.1 9001
"""

import socket
import threading
import argparse
import logging
import time
import json
from collections import deque
from datetime import datetime, timedelta

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9001
HISTORY_TTL_MINUTES = 15
PERSIST_FILE = "chat_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)

class ChatServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.stop_event = threading.Event()
        self.groups = {}  # group_id -> {"clients": set(), "history": deque()}
        self.lock = threading.Lock()

    def start(self):
        self._load_history()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("ChatServer listening on %s:%d", self.host, self.port)

        try:
            while not self.stop_event.is_set():
                client_sock, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt, shutting down...")
        finally:
            self.stop()

    def stop(self):
        self.stop_event.set()
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self._save_history()
        logging.info("Server stopped.")

    def _handle_client(self, conn, addr):
        logging.info("Client connected from %s", addr)
        try:
            f = conn.makefile("rwb")
            # handshake: expect "user_id group_id\n"
            line = f.readline().decode().strip()
            if not line:
                conn.close()
                return
            try:
                user_id, group_id = line.split()
            except ValueError:
                conn.sendall(b"ERROR: Expected 'user_id group_id'\n")
                conn.close()
                return

            with self.lock:
                if group_id not in self.groups:
                    self.groups[group_id] = {"clients": set(), "history": deque()}
                group = self.groups[group_id]
                group["clients"].add(conn)

            # replay recent history
            self._replay_history(conn, group_id)

            # announce join
            self._broadcast(group_id, f"[{user_id}] joined group {group_id}\n", exclude=conn)

            # main loop: relay messages
            while True:
                msg = f.readline()
                if not msg:
                    break
                text = msg.decode().rstrip("\n")
                timestamp = time.time()
                entry = (timestamp, user_id, text)
                self._store_message(group_id, entry)
                self._broadcast(group_id, f"[{user_id}] {text}\n", exclude=conn)

        except Exception as e:
            logging.exception("Error handling client %s", addr)
        finally:
            self._remove_client(conn)
            conn.close()
            logging.info("Client %s disconnected", addr)

    def _broadcast(self, group_id, message, exclude=None):
        with self.lock:
            clients = list(self.groups[group_id]["clients"])
        dead = []
        for c in clients:
            if c is exclude:
                continue
            try:
                c.sendall(message.encode())
            except Exception:
                dead.append(c)
        for d in dead:
            self._remove_client(d)

    def _store_message(self, group_id, entry):
        ts, user_id, msg = entry
        with self.lock:
            hist = self.groups[group_id]["history"]
            hist.append(entry)
            # cleanup old
            cutoff = time.time() - HISTORY_TTL_MINUTES * 60
            while hist and hist[0][0] < cutoff:
                hist.popleft()

    def _replay_history(self, conn, group_id):
        cutoff = time.time() - HISTORY_TTL_MINUTES * 60
        with self.lock:
            hist = list(self.groups[group_id]["history"])
        for ts, user_id, msg in hist:
            if ts >= cutoff:
                line = f"[{user_id} @ {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}] {msg}\n"
                try:
                    conn.sendall(line.encode())
                except Exception:
                    break

    def _remove_client(self, conn):
        with self.lock:
            for g, data in list(self.groups.items()):
                if conn in data["clients"]:
                    data["clients"].remove(conn)
                    if not data["clients"]:
                        del self.groups[g]

    def _save_history(self):
        data = {}
        with self.lock:
            for gid, g in self.groups.items():
                data[gid] = list(g["history"])
        try:
            with open(PERSIST_FILE, "w") as f:
                json.dump(data, f)
            logging.info("Saved history to %s", PERSIST_FILE)
        except Exception:
            logging.exception("Error saving history")

    def _load_history(self):
        try:
            with open(PERSIST_FILE) as f:
                data = json.load(f)
            now = time.time()
            cutoff = now - HISTORY_TTL_MINUTES * 60
            with self.lock:
                for gid, hist in data.items():
                    dq = deque()
                    for ts, user_id, msg in hist:
                        if ts >= cutoff:
                            dq.append((ts, user_id, msg))
                    self.groups[gid] = {"clients": set(), "history": dq}
            logging.info("Loaded history from %s", PERSIST_FILE)
        except FileNotFoundError:
            pass
        except Exception:
            logging.exception("Error loading history")


def main():
    ap = argparse.ArgumentParser(description="Multi-Group Chat Server")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = ap.parse_args()
    srv = ChatServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()
