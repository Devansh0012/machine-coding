#!/usr/bin/env python3
"""
http_file_server.py
Minimal HTTP/1.1 server with Range support
"""
import socket, threading, argparse, logging, os

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9004
ROOT_DIR = "."

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class HTTPServer:
    def __init__(self, host, port, root):
        self.host, self.port, self.root = host, port, root

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(50)
        logging.info("HTTP server on %s:%d", self.host, self.port)
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        try:
            req = conn.recv(4096).decode(errors="ignore")
            if not req: return
            line, *hdrs = req.split("\r\n")
            method, path, _ = line.split()
            if method != "GET":
                conn.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return
            filepath = os.path.join(self.root, path.lstrip("/"))
            if not os.path.isfile(filepath):
                conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
                return
            size = os.path.getsize(filepath)
            # Parse Range header
            start, end = 0, size - 1
            for h in hdrs:
                if h.lower().startswith("range:"):
                    _, val = h.split(":", 1)
                    rng = val.strip().split("=")[1]
                    start_s, end_s = rng.split("-")
                    if start_s: start = int(start_s)
                    if end_s: end = int(end_s)
            length = end - start + 1
            with open(filepath, "rb") as f:
                f.seek(start)
                data = f.read(length)
            status = b"206 Partial Content" if start > 0 or end < size-1 else b"200 OK"
            resp = (
                b"HTTP/1.1 " + status + b"\r\n" +
                f"Content-Length: {len(data)}\r\n".encode() +
                f"Content-Range: bytes {start}-{end}/{size}\r\n".encode() +
                b"Connection: close\r\n\r\n"
            )
            conn.sendall(resp + data)
        except Exception as e:
            logging.exception("Error handling %s", addr)
        finally:
            conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--root", default=ROOT_DIR)
    args = ap.parse_args()
    HTTPServer(args.host, args.port, args.root).start()

if __name__ == "__main__":
    main()
