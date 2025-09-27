#!/usr/bin/env python3
"""
echo_server.py

Simple TCP echo server (line-based) with thread-per-connection model.
Test with: telnet 127.0.0.1 9000  OR  nc 127.0.0.1 9000
"""

import socket
import threading
import argparse
import logging
import signal

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9000
MAX_LINE_BYTES = 4096
MAX_CONCURRENT_CLIENTS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)


class EchoServer:
    def __init__(self, host: str, port: int, max_clients: int = MAX_CONCURRENT_CLIENTS):
        self.host = host
        self.port = port
        self._sock = None
        self._stop_event = threading.Event()
        self._client_sema = threading.Semaphore(max_clients)
        self._threads = []

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(128)
        logging.info("EchoServer listening on %s:%d", self.host, self.port)

        # Accept loop
        try:
            while not self._stop_event.is_set():
                try:
                    client_sock, addr = self._sock.accept()
                except OSError:
                    break  # socket closed during shutdown
                if not self._client_sema.acquire(blocking=False):
                    # Too many clients
                    logging.warning("Max clients reached; rejecting %s", addr)
                    try:
                        client_sock.sendall(b"Server busy. Try later.\n")
                    finally:
                        client_sock.close()
                    continue

                t = threading.Thread(
                    target=self._handle_client, args=(client_sock, addr), daemon=True
                )
                t.start()
                self._threads.append(t)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received; shutting down")
        finally:
            self.stop()

    def stop(self):
        if self._stop_event.is_set():
            return
        logging.info("Stopping server...")
        self._stop_event.set()
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass

        # Optionally wait for threads for a short period
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=1.0)
        logging.info("Server stopped.")

    def _handle_client(self, conn: socket.socket, addr):
        thread_name = threading.current_thread().name
        logging.info("Client connected %s", addr)
        try:
            # Use a buffered file-like object for convenient readline semantics
            f = conn.makefile("rwb", buffering=0)
            # Note: 'buffering=0' ensures writes are not heavily buffered at Python level,
            # but makefile may still buffer; we flush explicitly.
            while True:
                # Read a line (blocks until newline or EOF)
                line = f.readline(MAX_LINE_BYTES)
                if not line:
                    # client closed connection
                    break
                
                # Log message received from client
                logging.info("Message received from client %s: %s", addr, line.decode('utf-8', errors='replace').strip())
                
                # Echo back the exact bytes received
                try:
                    f.write(line)
                    f.flush()
                    # Log message sent to client
                    logging.info("Message sent to client %s: %s", addr, line.decode('utf-8', errors='replace').strip())
                except BrokenPipeError:
                    break
                except Exception:
                    logging.exception("Error sending to client %s", addr)
                    break
        except Exception:
            logging.exception("Exception in client handler for %s", addr)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # release client slot
            try:
                self._client_sema.release()
            except Exception:
                pass
            logging.info("Client disconnected %s", addr)


def parse_args():
    ap = argparse.ArgumentParser(description="Threaded TCP Echo Server")
    ap.add_argument("--host", default=DEFAULT_HOST, help="Host to bind (default all interfaces)")
    ap.add_argument("--port", default=DEFAULT_PORT, type=int, help="Port to bind (default 9000)")
    ap.add_argument("--max-clients", default=MAX_CONCURRENT_CLIENTS, type=int, help="Max concurrent clients")
    return ap.parse_args()


def main():
    args = parse_args()
    srv = EchoServer(args.host, args.port, max_clients=args.max_clients)

    def _signal_handler(sig, frame):
        logging.info("Signal %s received, shutting down...", sig)
        srv.stop()
        # allow main thread to exit
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    srv.start()


if __name__ == "__main__":
    main()
