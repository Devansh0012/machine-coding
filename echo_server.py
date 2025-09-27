#!/usr/bin/env python3  # Shebang line to specify Python 3 interpreter
"""
echo_server.py

Simple TCP echo server (line-based) with thread-per-connection model.
Test with: telnet 127.0.0.1 9000  OR  nc 127.0.0.1 9000
"""

import socket      # For TCP socket operations
import threading   # For thread-per-connection model
import argparse    # For command-line argument parsing
import logging     # For structured logging
import signal      # For graceful signal handling

DEFAULT_HOST = "0.0.0.0"              # Default host to bind to all interfaces
DEFAULT_PORT = 9000                   # Default TCP port
MAX_LINE_BYTES = 4096                 # Maximum bytes to read per line
MAX_CONCURRENT_CLIENTS = 2            # Default maximum concurrent connections

# Configure logging format and level
logging.basicConfig(
    level=logging.INFO,               # Set logging level to INFO
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",  # Log format with timestamp, level, thread name
)


class EchoServer:
    def __init__(self, host: str, port: int, max_clients: int = MAX_CONCURRENT_CLIENTS):
        self.host = host                                          # Store host address
        self.port = port                                          # Store port number
        self._sock = None                                         # Initialize socket to None
        self._stop_event = threading.Event()                     # Event to signal server shutdown
        self._client_sema = threading.Semaphore(max_clients)     # Semaphore to limit concurrent clients
        self._threads = []                                        # List to track client handler threads

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow address reuse
        self._sock.bind((self.host, self.port))                          # Bind socket to host:port
        self._sock.listen(128)                                           # Start listening with backlog of 128
        logging.info("EchoServer listening on %s:%d", self.host, self.port)  # Log server start

        # Accept loop
        try:
            while not self._stop_event.is_set():                        # Loop until stop event is set
                try:
                    client_sock, addr = self._sock.accept()             # Accept incoming connection
                except OSError:
                    break  # socket closed during shutdown               # Break if socket closed during shutdown
                if not self._client_sema.acquire(blocking=False):       # Try to acquire client slot (non-blocking)
                    # Too many clients
                    logging.warning("Max clients reached; rejecting %s", addr)  # Log client rejection
                    try:
                        client_sock.sendall(b"Server busy. Try later.\n")       # Send busy message to client
                    finally:
                        client_sock.close()                                      # Close client socket
                    continue                                                     # Continue to next iteration

                # Create new thread for client handling
                t = threading.Thread(
                    target=self._handle_client, args=(client_sock, addr), daemon=True  # Create daemon thread
                )
                t.start()                                                # Start the thread
                self._threads.append(t)                                  # Add thread to tracking list
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received; shutting down")   # Log keyboard interrupt
        finally:
            self.stop()                                                  # Ensure server stops in finally block

    def stop(self):
        if self._stop_event.is_set():                                    # Check if already stopping
            return                                                       # Return early if already stopping
        logging.info("Stopping server...")                              # Log server stopping
        self._stop_event.set()                                           # Set stop event to signal shutdown
        try:
            if self._sock:                                               # Check if socket exists
                self._sock.close()                                       # Close the server socket
        except Exception:
            pass                                                         # Ignore exceptions during socket close

        # Optionally wait for threads for a short period
        for t in self._threads:                                          # Iterate through all client threads
            if t.is_alive():                                             # Check if thread is still alive
                t.join(timeout=1.0)                                      # Wait up to 1 second for thread to finish
        logging.info("Server stopped.")                                 # Log server stopped

    def _handle_client(self, conn: socket.socket, addr):
        thread_name = threading.current_thread().name                   # Get current thread name
        logging.info("Client connected %s", addr)                       # Log client connection
        try:
            # Use a buffered file-like object for convenient readline semantics
            f = conn.makefile("rwb", buffering=0)                        # Create file-like object from socket
            # Note: 'buffering=0' ensures writes are not heavily buffered at Python level,
            # but makefile may still buffer; we flush explicitly.
            while True:                                                  # Main client handling loop
                # Read a line (blocks until newline or EOF)
                line = f.readline(MAX_LINE_BYTES)                        # Read line up to MAX_LINE_BYTES
                if not line:                                             # Check if client closed connection
                    # client closed connection
                    break                                                # Exit loop if no data received
                
                # Log message received from client
                logging.info("Message received from client %s: %s", addr, line.decode('utf-8', errors='replace').strip())
                
                # Echo back the exact bytes received
                try:
                    f.write(line)                                        # Write line back to client
                    f.flush()                                            # Flush to ensure data is sent
                    # Log message sent to client
                    logging.info("Message sent to client %s: %s", addr, line.decode('utf-8', errors='replace').strip())
                except BrokenPipeError:
                    break                                                # Exit if client disconnected
                except Exception:
                    logging.exception("Error sending to client %s", addr)  # Log any other send errors
                    break                                                # Exit on error
        except Exception:
            logging.exception("Exception in client handler for %s", addr)  # Log any handler exceptions
        finally:
            try:
                conn.close()                                             # Close client connection
            except Exception:
                pass                                                     # Ignore close exceptions
            # release client slot
            try:
                self._client_sema.release()                              # Release semaphore slot
            except Exception:
                pass                                                     # Ignore release exceptions
            logging.info("Client disconnected %s", addr)                # Log client disconnection


def parse_args():
    ap = argparse.ArgumentParser(description="Threaded TCP Echo Server")  # Create argument parser
    ap.add_argument("--host", default=DEFAULT_HOST, help="Host to bind (default all interfaces)")  # Host argument
    ap.add_argument("--port", default=DEFAULT_PORT, type=int, help="Port to bind (default 9000)")  # Port argument
    ap.add_argument("--max-clients", default=MAX_CONCURRENT_CLIENTS, type=int, help="Max concurrent clients")  # Max clients argument
    return ap.parse_args()                                               # Parse and return arguments


def main():
    args = parse_args()                                                  # Parse command line arguments
    srv = EchoServer(args.host, args.port, max_clients=args.max_clients)  # Create server instance

    def _signal_handler(sig, frame):                                     # Define signal handler function
        logging.info("Signal %s received, shutting down...", sig)       # Log signal reception
        srv.stop()                                                       # Stop the server
        # allow main thread to exit
    signal.signal(signal.SIGINT, _signal_handler)                       # Register SIGINT handler (Ctrl+C)
    signal.signal(signal.SIGTERM, _signal_handler)                      # Register SIGTERM handler (termination)

    srv.start()                                                          # Start the server


if __name__ == "__main__":                                              # Check if script is run directly
    main()                                                               # Call main function


"""
CODE SUMMARY:
This is a multi-threaded TCP echo server implementation in Python that demonstrates
concurrent client handling using a thread-per-connection model.

Key Features:
1. TCP Server: Creates a TCP socket that listens on a configurable host:port
2. Thread-per-connection: Spawns a new daemon thread for each client connection
3. Concurrent client limiting: Uses a semaphore to limit maximum concurrent clients
4. Echo functionality: Reads lines from clients and echoes them back exactly
5. Graceful shutdown: Handles SIGINT/SIGTERM signals for clean server shutdown
6. Comprehensive logging: Logs all connections, messages, and server events
7. Command-line interface: Accepts host, port, and max-clients as arguments
8. Error handling: Robust exception handling for network and threading errors

Architecture:
- EchoServer class encapsulates server functionality
- Main thread runs the accept loop and creates client handler threads
- Each client gets a dedicated thread that handles read/write operations
- Semaphore ensures server doesn't get overwhelmed by too many clients
- Signal handlers allow graceful shutdown on system signals

Usage: Run with default settings or customize with --host, --port, --max-clients
Test with: telnet 127.0.0.1 9000 or nc 127.0.0.1 9000
"""
