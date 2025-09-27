#!/usr/bin/env python3
"""
pubsub_server.py
TCP Pub/Sub Broker
"""
import socket, threading, argparse, logging, time
from collections import deque

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9003
HISTORY_LIMIT = 50

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class PubSubServer:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.sock = None
        self.topics = {}  # topic -> {"subs": set(conns), "hist": deque()}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("PubSubServer listening on %s:%d", self.host, self.port)
        try:
            while not self.stop_event.is_set():
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Stopping...")
        finally:
            self.stop()

    def stop(self):
        self.stop_event.set()
        if self.sock: self.sock.close()

    def _handle_client(self, conn, addr):
        logging.info("Client %s connected", addr)
        f = conn.makefile("rwb")
        try:
            while True:
                line = f.readline()
                if not line: break
                parts = line.decode().strip().split(maxsplit=2)
                if not parts: continue
                cmd = parts[0].upper()
                if cmd == "SUBSCRIBE" and len(parts) == 2:
                    topic = parts[1]
                    self._subscribe(conn, topic)
                elif cmd == "PUBLISH" and len(parts) == 3:
                    topic, msg = parts[1], parts[2]
                    self._publish(topic, msg)
                    conn.sendall(b"OK\n")
                else:
                    conn.sendall(b"ERROR: Unknown command\n")
        finally:
            self._remove_client(conn)
            conn.close()
            logging.info("Client %s disconnected", addr)

    def _subscribe(self, conn, topic):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = {"subs": set(), "hist": deque()}
            self.topics[topic]["subs"].add(conn)
            history = list(self.topics[topic]["hist"])
        # Replay
        for msg in history:
            try: conn.sendall((msg + "\n").encode())
            except: pass
        conn.sendall(b"SUBSCRIBED\n")

    def _publish(self, topic, msg):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = {"subs": set(), "hist": deque()}
            hist = self.topics[topic]["hist"]
            hist.append(msg)
            if len(hist) > HISTORY_LIMIT:
                hist.popleft()
            subs = list(self.topics[topic]["subs"])
        for c in subs:
            try: c.sendall((msg + "\n").encode())
            except: self._remove_client(c)

    def _remove_client(self, conn):
        with self.lock:
            for t, d in self.topics.items():
                if conn in d["subs"]:
                    d["subs"].remove(conn)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = ap.parse_args()
    srv = PubSubServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()
