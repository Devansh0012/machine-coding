#!/usr/bin/env python3
"""
file_tail_server.py

TCP File Tail Server:
- Streams file contents in real-time
- Supports substring filters per client
- Multiple clients, multiple files
"""

import socket
import threading
import argparse
import logging
import os
import time

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9002

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)

class FileTailServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.stop_event = threading.Event()
        self.subscriptions = {}  # file -> {"clients": set((conn, filter_str)), "thread": t}
        self.lock = threading.Lock()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("FileTailServer listening on %s:%d", self.host, self.port)

        try:
            while not self.stop_event.is_set():
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
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
        logging.info("Server stopped.")

    def _handle_client(self, conn, addr):
        logging.info("Client connected %s", addr)
        try:
            f = conn.makefile("rwb")
            # First line: filepath [substring]
            line = f.readline().decode().strip()
            if not line:
                conn.close()
                return
            parts = line.split(maxsplit=1)
            filepath = parts[0]
            filter_str = parts[1] if len(parts) > 1 else None

            if not os.path.isfile(filepath):
                conn.sendall(b"ERROR: File not found\n")
                conn.close()
                return

            # Subscribe client
            with self.lock:
                if filepath not in self.subscriptions:
                    self.subscriptions[filepath] = {"clients": set(), "thread": None}
                self.subscriptions[filepath]["clients"].add((conn, filter_str))
                if not self.subscriptions[filepath]["thread"]:
                    t = threading.Thread(target=self._file_reader, args=(filepath,), daemon=True)
                    self.subscriptions[filepath]["thread"] = t
                    t.start()

            # Block until client disconnects
            while True:
                if not f.readline():
                    break
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            self._remove_client(conn)
            conn.close()
            logging.info("Client %s disconnected", addr)

    def _file_reader(self, filepath):
        logging.info("Starting file reader for %s", filepath)
        try:
            with open(filepath, "r") as f:
                # Seek to end
                f.seek(0, os.SEEK_END)
                while True:
                    line = f.readline()
                    if not line:
                        if self.stop_event.is_set():
                            break
                        time.sleep(0.5)
                        continue
                    line = line.rstrip("\n")
                    # Broadcast to clients
                    with self.lock:
                        clients = list(self.subscriptions.get(filepath, {}).get("clients", []))
                    dead = []
                    for conn, filter_str in clients:
                        if filter_str and filter_str not in line:
                            continue
                        try:
                            conn.sendall((line + "\n").encode())
                        except Exception:
                            dead.append(conn)
                    for d in dead:
                        self._remove_client(d)
        except Exception:
            logging.exception("Error in file reader for %s", filepath)
        finally:
            logging.info("Stopping file reader for %s", filepath)

    def _remove_client(self, conn):
        with self.lock:
            for fp, data in list(self.subscriptions.items()):
                data["clients"] = {c for c in data["clients"] if c[0] != conn}
                if not data["clients"]:
                    self.subscriptions.pop(fp)
                    # Thread will exit naturally since no clients remain


def main():
    ap = argparse.ArgumentParser(description="File Tail Server")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = ap.parse_args()
    srv = FileTailServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()
