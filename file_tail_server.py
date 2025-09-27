#!/usr/bin/env python3
"""
file_tail_server.py

TCP File Tail Server:
- Streams file contents in real-time
- Supports substring filters per client
- Multiple clients, multiple files
"""

import socket  # For TCP socket operations
import threading  # For handling multiple clients concurrently
import argparse  # For parsing command line arguments
import logging  # For logging server events and errors
import os  # For file system operations
import time  # For sleep operations in file reading loop

DEFAULT_HOST = "0.0.0.0"  # Default host to bind to (all interfaces)
DEFAULT_PORT = 9002  # Default port number for the server

# Configure logging with timestamp, level, thread name, and message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)

class FileTailServer:
    def __init__(self, host, port):
        self.host = host  # Server host address
        self.port = port  # Server port number
        self.sock = None  # Server socket object
        self.stop_event = threading.Event()  # Event to signal server shutdown
        self.subscriptions = {}  # Dictionary mapping files to client data: file -> {"clients": set((conn, filter_str)), "thread": t}
        self.lock = threading.Lock()  # Lock for thread-safe access to subscriptions

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow socket reuse
        self.sock.bind((self.host, self.port))  # Bind socket to host and port
        self.sock.listen(100)  # Listen for incoming connections with backlog of 100
        logging.info("FileTailServer listening on %s:%d", self.host, self.port)  # Log server start

        try:
            while not self.stop_event.is_set():  # Main server loop until stop event is set
                conn, addr = self.sock.accept()  # Accept incoming client connection
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()  # Start new thread for each client
        except KeyboardInterrupt:  # Handle Ctrl+C gracefully
            logging.info("KeyboardInterrupt, shutting down...")  # Log shutdown message
        finally:
            self.stop()  # Ensure server cleanup happens

    def stop(self):
        self.stop_event.set()  # Signal all threads to stop
        try:
            if self.sock:  # Check if socket exists
                self.sock.close()  # Close the server socket
        except Exception:  # Ignore any errors during socket closure
            pass
        logging.info("Server stopped.")  # Log server shutdown

    def _handle_client(self, conn, addr):
        logging.info("Client connected %s", addr)  # Log new client connection
        try:
            f = conn.makefile("rwb")  # Create file-like object from socket for easier I/O
            # First line: filepath [substring]
            line = f.readline().decode().strip()  # Read first line from client (file path and optional filter)
            if not line:  # If no data received
                conn.close()  # Close connection
                return  # Exit function
            parts = line.split(maxsplit=1)  # Split line into at most 2 parts
            filepath = parts[0]  # First part is the file path
            filter_str = parts[1] if len(parts) > 1 else None  # Second part is optional filter string

            if not os.path.isfile(filepath):  # Check if file exists
                conn.sendall(b"ERROR: File not found\n")  # Send error message to client
                conn.close()  # Close connection
                return  # Exit function

            # Subscribe client
            with self.lock:  # Acquire lock for thread-safe operations
                if filepath not in self.subscriptions:  # If this is first client for this file
                    self.subscriptions[filepath] = {"clients": set(), "thread": None}  # Initialize subscription data
                self.subscriptions[filepath]["clients"].add((conn, filter_str))  # Add client to subscription list
                if not self.subscriptions[filepath]["thread"]:  # If no file reader thread exists
                    t = threading.Thread(target=self._file_reader, args=(filepath,), daemon=True)  # Create file reader thread
                    self.subscriptions[filepath]["thread"] = t  # Store thread reference
                    t.start()  # Start the file reader thread

            # Block until client disconnects
            while True:  # Keep connection alive
                if not f.readline():  # Read from client (blocking) - empty read means disconnect
                    break  # Exit loop when client disconnects
        except Exception:  # Handle any errors in client handling
            logging.exception("Error handling client %s", addr)  # Log error with stack trace
        finally:
            self._remove_client(conn)  # Remove client from all subscriptions
            conn.close()  # Close client connection
            logging.info("Client %s disconnected", addr)  # Log client disconnection

    def _file_reader(self, filepath):
        logging.info("Starting file reader for %s", filepath)  # Log file reader start
        try:
            with open(filepath, "r") as f:  # Open file for reading
                # Seek to end
                f.seek(0, os.SEEK_END)  # Move to end of file to only read new content
                while True:  # Main file reading loop
                    line = f.readline()  # Read next line from file
                    if not line:  # If no new content
                        if self.stop_event.is_set():  # Check if server is shutting down
                            break  # Exit loop
                        time.sleep(0.5)  # Wait before checking for new content
                        continue  # Continue to next iteration
                    line = line.rstrip("\n")  # Remove trailing newline
                    # Broadcast to clients
                    with self.lock:  # Acquire lock for thread-safe access
                        clients = list(self.subscriptions.get(filepath, {}).get("clients", []))  # Get copy of client list
                    dead = []  # List to track disconnected clients
                    for conn, filter_str in clients:  # Iterate through all subscribed clients
                        if filter_str and filter_str not in line:  # If client has filter and line doesn't match
                            continue  # Skip this client
                        try:
                            conn.sendall((line + "\n").encode())  # Send line to client with newline
                        except Exception:  # If send fails (client disconnected)
                            dead.append(conn)  # Mark client as dead
                    for d in dead:  # Remove all dead clients
                        self._remove_client(d)  # Clean up dead client
        except Exception:  # Handle any errors in file reading
            logging.exception("Error in file reader for %s", filepath)  # Log error with stack trace
        finally:
            logging.info("Stopping file reader for %s", filepath)  # Log file reader stop

    def _remove_client(self, conn):
        with self.lock:  # Acquire lock for thread-safe operations
            for fp, data in list(self.subscriptions.items()):  # Iterate through all file subscriptions
                data["clients"] = {c for c in data["clients"] if c[0] != conn}  # Remove client from subscription
                if not data["clients"]:  # If no clients left for this file
                    self.subscriptions.pop(fp)  # Remove file subscription entirely
                    # Thread will exit naturally since no clients remain


def main():
    ap = argparse.ArgumentParser(description="File Tail Server")  # Create argument parser
    ap.add_argument("--host", default=DEFAULT_HOST)  # Add host argument with default
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)  # Add port argument with default
    args = ap.parse_args()  # Parse command line arguments
    srv = FileTailServer(args.host, args.port)  # Create server instance
    srv.start()  # Start the server

if __name__ == "__main__":  # If script is run directly (not imported)
    main()  # Call main function

"""
CODE SUMMARY:

This is a TCP File Tail Server that provides real-time streaming of file contents to multiple clients.

Key Features:
1. Multi-client support: Multiple clients can connect simultaneously
2. Multi-file support: Different clients can tail different files
3. Substring filtering: Clients can specify a filter string to only receive lines containing that substring
4. Real-time streaming: New content is pushed to clients as it's written to files

Architecture:
- Main server thread accepts new client connections
- Each client gets its own handler thread
- Each unique file gets its own reader thread that tails the file
- Thread-safe subscription management using locks

Protocol:
- Client connects and sends: "filepath [optional_filter_string]"
- Server validates file exists, subscribes client, and starts streaming new lines
- Lines are filtered per client's substring requirement
- Connection stays open until client disconnects

Thread Management:
- File reader threads are created on-demand when first client subscribes to a file
- File reader threads terminate when no clients remain for that file
- All threads are marked as daemon threads for clean shutdown

Error Handling:
- Graceful handling of client disconnections
- File not found errors sent to client
- Comprehensive logging for debugging
- Clean resource cleanup on shutdown
"""
