#!/usr/bin/env python3  # Shebang line to specify Python 3 interpreter
"""
multi_group_chat_server.py

TCP Group Chat Server with:
- Multiple groups
- History replay (last 15 min)
- Persistence to JSON
Test with: telnet 127.0.0.1 9001
"""

import socket  # Network communication functionality
import threading  # Multi-threading support for concurrent clients
import argparse  # Command line argument parsing
import logging  # Logging system for debugging and monitoring
import time  # Time-related functions for timestamps
import json  # JSON serialization for data persistence
from collections import deque  # Double-ended queue for efficient message history storage
from datetime import datetime, timedelta  # Date and time manipulation

DEFAULT_HOST = "0.0.0.0"  # Default server host (listen on all interfaces)
DEFAULT_PORT = 9001  # Default server port
HISTORY_TTL_MINUTES = 15  # Time-to-live for message history in minutes
PERSIST_FILE = "chat_history.json"  # File name for persisting chat history

# Configure logging with timestamp, level, thread name, and message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)

class ChatServer:  # Main chat server class
    def __init__(self, host, port):  # Constructor to initialize server
        self.host = host  # Store server host address
        self.port = port  # Store server port number
        self.sock = None  # Initialize socket object as None
        self.stop_event = threading.Event()  # Event to signal server shutdown
        self.groups = {}  # Dictionary to store groups: group_id -> {"clients": set(), "history": deque()}
        self.lock = threading.Lock()  # Thread lock for safe access to shared data

    def start(self):  # Method to start the server
        self._load_history()  # Load previous chat history from file
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow socket reuse
        self.sock.bind((self.host, self.port))  # Bind socket to host and port
        self.sock.listen(100)  # Start listening for connections (max 100 pending)
        logging.info("ChatServer listening on %s:%d", self.host, self.port)  # Log server start

        try:
            while not self.stop_event.is_set():  # Main server loop until stop signal
                client_sock, addr = self.sock.accept()  # Accept new client connection
                # Create new thread to handle each client connection
                threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True).start()
        except KeyboardInterrupt:  # Handle Ctrl+C gracefully
            logging.info("KeyboardInterrupt, shutting down...")
        finally:
            self.stop()  # Clean up and stop server

    def stop(self):  # Method to stop the server
        self.stop_event.set()  # Signal all threads to stop
        try:
            if self.sock:  # If socket exists
                self.sock.close()  # Close the server socket
        except Exception:  # Ignore any exceptions during cleanup
            pass
        self._save_history()  # Save current chat history to file
        logging.info("Server stopped.")  # Log server shutdown

    def _handle_client(self, conn, addr):  # Handle individual client connections
        logging.info("Client connected from %s", addr)  # Log new client connection
        try:
            f = conn.makefile("rwb")  # Create file-like object for easier I/O
            # handshake: expect "user_id group_id\n"
            line = f.readline().decode().strip()  # Read first line from client
            if not line:  # If empty line received
                conn.close()  # Close connection
                return
            try:
                user_id, group_id = line.split()  # Parse user_id and group_id
            except ValueError:  # If parsing fails
                conn.sendall(b"ERROR: Expected 'user_id group_id'\n")  # Send error message
                conn.close()  # Close connection
                return

            with self.lock:  # Acquire thread lock for safe access
                if group_id not in self.groups:  # If group doesn't exist
                    self.groups[group_id] = {"clients": set(), "history": deque()}  # Create new group
                group = self.groups[group_id]  # Get reference to the group
                group["clients"].add(conn)  # Add client connection to group

            # replay recent history
            self._replay_history(conn, group_id)  # Send historical messages to new client

            # announce join
            self._broadcast(group_id, f"[{user_id}] joined group {group_id}\n", exclude=conn)  # Notify others of join

            # main loop: relay messages
            while True:  # Message relay loop
                msg = f.readline()  # Read message from client
                if not msg:  # If no message (client disconnected)
                    break
                text = msg.decode().rstrip("\n")  # Decode and clean message text
                timestamp = time.time()  # Get current timestamp
                entry = (timestamp, user_id, text)  # Create message entry tuple
                self._store_message(group_id, entry)  # Store message in history
                self._broadcast(group_id, f"[{user_id}] {text}\n", exclude=conn)  # Broadcast to other clients

        except Exception as e:  # Handle any exceptions during client handling
            logging.exception("Error handling client %s", addr)  # Log the exception
        finally:
            self._remove_client(conn)  # Remove client from all groups
            conn.close()  # Close client connection
            logging.info("Client %s disconnected", addr)  # Log client disconnection

    def _broadcast(self, group_id, message, exclude=None):  # Broadcast message to group members
        with self.lock:  # Acquire thread lock
            clients = list(self.groups[group_id]["clients"])  # Get list of clients in group
        dead = []  # List to track disconnected clients
        for c in clients:  # Iterate through all clients
            if c is exclude:  # Skip the excluded client (usually sender)
                continue
            try:
                c.sendall(message.encode())  # Send message to client
            except Exception:  # If sending fails (client disconnected)
                dead.append(c)  # Mark client as dead
        for d in dead:  # Remove all dead clients
            self._remove_client(d)

    def _store_message(self, group_id, entry):  # Store message in group history
        ts, user_id, msg = entry  # Unpack message entry
        with self.lock:  # Acquire thread lock
            hist = self.groups[group_id]["history"]  # Get group history deque
            hist.append(entry)  # Add new message to history
            # cleanup old
            cutoff = time.time() - HISTORY_TTL_MINUTES * 60  # Calculate cutoff time
            while hist and hist[0][0] < cutoff:  # Remove messages older than TTL
                hist.popleft()

    def _replay_history(self, conn, group_id):  # Send historical messages to client
        cutoff = time.time() - HISTORY_TTL_MINUTES * 60  # Calculate cutoff time
        with self.lock:  # Acquire thread lock
            hist = list(self.groups[group_id]["history"])  # Get copy of history
        for ts, user_id, msg in hist:  # Iterate through historical messages
            if ts >= cutoff:  # If message is within TTL
                # Format message with timestamp
                line = f"[{user_id} @ {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}] {msg}\n"
                try:
                    conn.sendall(line.encode())  # Send historical message to client
                except Exception:  # If sending fails
                    break

    def _remove_client(self, conn):  # Remove client from all groups
        with self.lock:  # Acquire thread lock
            for g, data in list(self.groups.items()):  # Iterate through all groups
                if conn in data["clients"]:  # If client is in this group
                    data["clients"].remove(conn)  # Remove client from group
                    if not data["clients"]:  # If group becomes empty
                        del self.groups[g]  # Delete the empty group

    def _save_history(self):  # Save chat history to JSON file
        data = {}  # Dictionary to hold serializable data
        with self.lock:  # Acquire thread lock
            for gid, g in self.groups.items():  # Iterate through all groups
                data[gid] = list(g["history"])  # Convert deque to list for JSON serialization
        try:
            with open(PERSIST_FILE, "w") as f:  # Open file for writing
                json.dump(data, f)  # Write data as JSON
            logging.info("Saved history to %s", PERSIST_FILE)  # Log successful save
        except Exception:  # Handle any file I/O errors
            logging.exception("Error saving history")  # Log the error

    def _load_history(self):  # Load chat history from JSON file
        try:
            with open(PERSIST_FILE) as f:  # Open file for reading
                data = json.load(f)  # Load JSON data
            now = time.time()  # Get current time
            cutoff = now - HISTORY_TTL_MINUTES * 60  # Calculate cutoff time
            with self.lock:  # Acquire thread lock
                for gid, hist in data.items():  # Iterate through loaded groups
                    dq = deque()  # Create new deque for history
                    for ts, user_id, msg in hist:  # Iterate through messages
                        if ts >= cutoff:  # If message is within TTL
                            dq.append((ts, user_id, msg))  # Add to history
                    self.groups[gid] = {"clients": set(), "history": dq}  # Create group with history
            logging.info("Loaded history from %s", PERSIST_FILE)  # Log successful load
        except FileNotFoundError:  # If file doesn't exist (first run)
            pass  # Do nothing, start with empty history
        except Exception:  # Handle any other errors
            logging.exception("Error loading history")  # Log the error


def main():  # Main function to run the server
    ap = argparse.ArgumentParser(description="Multi-Group Chat Server")  # Create argument parser
    ap.add_argument("--host", default=DEFAULT_HOST)  # Add host argument
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)  # Add port argument
    args = ap.parse_args()  # Parse command line arguments
    srv = ChatServer(args.host, args.port)  # Create server instance
    srv.start()  # Start the server

if __name__ == "__main__":  # If script is run directly
    main()  # Call main function

"""
SUMMARY:
This code implements a multi-group TCP chat server with the following features:

1. **Multi-Group Support**: Clients can join different chat groups by specifying a group_id during connection
2. **Concurrent Connections**: Uses threading to handle multiple clients simultaneously
3. **Message History**: Maintains last 15 minutes of messages per group using deques for efficient storage
4. **History Replay**: New clients receive recent message history when joining a group
5. **Persistence**: Chat history is saved to/loaded from JSON file for persistence across server restarts
6. **Real-time Broadcasting**: Messages are instantly broadcast to all clients in the same group
7. **Graceful Cleanup**: Handles client disconnections and server shutdown properly
8. **Connection Protocol**: Clients connect via TCP and send "user_id group_id" as handshake

The server listens on TCP port 9001 by default and can be tested using telnet. Each client thread handles
one connection, managing message relay, history storage, and group membership. Thread safety is ensured
using locks for shared data access.
"""
