#!/usr/bin/env python3
"""
health_check_server.py

Health Check Aggregator:
- Periodically checks endpoints (TCP/HTTP)
- Tracks history and computes health
- Exposes JSON status over HTTP
"""

import socket, threading, argparse, logging, time, json, http.server, socketserver
from collections import deque

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

CHECK_INTERVAL = 5
WINDOW_SIZE = 5
HEALTH_THRESHOLD = 0.8

class HealthAggregator:
    def __init__(self, services):
        # services is list of ("tcp", host, port)
        self.services = {self._key(s): {"conf": s, "history": deque(maxlen=WINDOW_SIZE), "last_status": None}
                         for s in services}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def _key(self, s):  # ("tcp", host, port) -> "tcp:host:port"
        return f"{s[0]}:{s[1]}:{s[2]}"

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while not self.stop_event.is_set():
            threads = []
            for k, svc in list(self.services.items()):
                t = threading.Thread(target=self._check_service, args=(svc,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads: t.join()
            time.sleep(CHECK_INTERVAL)

    def _check_service(self, svc):
        proto, host, port = svc["conf"]
        ok = 0
        try:
            if proto == "tcp":
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                res = sock.connect_ex((host, port))
                sock.close()
                ok = 1 if res == 0 else 0
            elif proto == "http":
                import http.client
                conn = http.client.HTTPConnection(host, port, timeout=2)
                conn.request("GET", "/")
                r = conn.getresponse()
                ok = 1 if r.status == 200 else 0
                conn.close()
        except Exception:
            ok = 0

        with self.lock:
            svc["history"].append(ok)
            ratio = sum(svc["history"]) / len(svc["history"])
            status = "healthy" if ratio >= HEALTH_THRESHOLD else "unhealthy"
            if svc["last_status"] != status:
                logging.info("Service %s changed to %s", svc["conf"], status)
            svc["last_status"] = status

    def get_status(self):
        with self.lock:
            result = {}
            for k, svc in self.services.items():
                ratio = sum(svc["history"]) / len(svc["history"]) if svc["history"] else 0
                result[k] = {"status": svc["last_status"], "success_ratio": ratio}
            return result

# HTTP Handler to expose health
class HealthHandler(http.server.BaseHTTPRequestHandler):
    aggregator = None
    def do_GET(self):
        if self.path == "/health":
            data = json.dumps(self.aggregator.get_status()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

def run_server(host, port, aggregator):
    HealthHandler.aggregator = aggregator
    with socketserver.ThreadingTCPServer((host, port), HealthHandler) as httpd:
        logging.info("HTTP API serving on %s:%d", host, port)
        httpd.serve_forever()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9005)
    args = ap.parse_args()

    services = [
        ("tcp", "google.com", 80),
        ("http", "example.com", 80),
    ]

    agg = HealthAggregator(services)
    agg.start()
    run_server(args.host, args.port, agg)

if __name__ == "__main__":
    main()
