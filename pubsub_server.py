#!/usr/bin/env python3
"""
pubsub_server.py
TCP Pub/Sub Broker
"""
import socket, threading, argparse, logging, time  # Import necessary modules for networking, threading, CLI args, logging, and time
from collections import deque  # Import deque for efficient queue operations

DEFAULT_HOST = "0.0.0.0"  # Default host to bind to (all interfaces)
DEFAULT_PORT = 9003  # Default port number for the server
HISTORY_LIMIT = 50  # Maximum number of messages to keep in history per topic

# Configure logging with timestamp, level, thread name, and message
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class PubSubServer:
    def __init__(self, host, port):
        self.host, self.port = host, port  # Store host and port for server binding
        self.sock = None  # Initialize socket as None
        self.topics = {}  # Dictionary to store topics with subscribers and message history
        self.lock = threading.Lock()  # Lock for thread-safe access to shared data
        self.stop_event = threading.Event()  # Event to signal server shutdown

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow socket reuse
        self.sock.bind((self.host, self.port))  # Bind socket to host and port
        self.sock.listen(100)  # Listen for incoming connections with backlog of 100
        logging.info("PubSubServer listening on %s:%d", self.host, self.port)  # Log server start
        try:
            while not self.stop_event.is_set():  # Main server loop until stop event is set
                conn, addr = self.sock.accept()  # Accept incoming client connection
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()  # Start new thread for each client
        except KeyboardInterrupt:  # Handle Ctrl+C gracefully
            logging.info("Stopping...")  # Log shutdown message
        finally:
            self.stop()  # Ensure cleanup happens

    def stop(self):
        self.stop_event.set()  # Set stop event to signal threads to stop
        if self.sock: self.sock.close()  # Close server socket if it exists

    def _handle_client(self, conn, addr):
        logging.info("Client %s connected", addr)  # Log new client connection
        f = conn.makefile("rwb")  # Create file-like object for easier reading/writing
        try:
            while True:  # Client handling loop
                line = f.readline()  # Read line from client
                if not line: break  # Break if connection closed
                parts = line.decode().strip().split(maxsplit=2)  # Parse command into parts
                if not parts: continue  # Skip empty lines
                cmd = parts[0].upper()  # Get command in uppercase
                if cmd == "SUBSCRIBE" and len(parts) == 2:  # Handle SUBSCRIBE command
                    topic = parts[1]  # Extract topic name
                    self._subscribe(conn, topic)  # Subscribe client to topic
                elif cmd == "PUBLISH" and len(parts) == 3:  # Handle PUBLISH command
                    topic, msg = parts[1], parts[2]  # Extract topic and message
                    self._publish(topic, msg)  # Publish message to topic
                    conn.sendall(b"OK\n")  # Send acknowledgment to publisher
                else:
                    conn.sendall(b"ERROR: Unknown command\n")  # Send error for unknown commands
        finally:
            self._remove_client(conn)  # Clean up client from all subscriptions
            conn.close()  # Close client connection
            logging.info("Client %s disconnected", addr)  # Log client disconnection

    def _subscribe(self, conn, topic):
        with self.lock:  # Acquire lock for thread safety
            if topic not in self.topics:  # Create topic if it doesn't exist
                self.topics[topic] = {"subs": set(), "hist": deque()}  # Initialize with empty subscribers and history
            self.topics[topic]["subs"].add(conn)  # Add connection to topic subscribers
            history = list(self.topics[topic]["hist"])  # Get copy of message history
        # Replay historical messages to new subscriber
        for msg in history:  # Iterate through message history
            try: conn.sendall((msg + "\n").encode())  # Send each historical message
            except: pass  # Ignore send failures
        conn.sendall(b"SUBSCRIBED\n")  # Confirm subscription to client

    def _publish(self, topic, msg):
        with self.lock:  # Acquire lock for thread safety
            if topic not in self.topics:  # Create topic if it doesn't exist
                self.topics[topic] = {"subs": set(), "hist": deque()}  # Initialize with empty subscribers and history
            hist = self.topics[topic]["hist"]  # Get reference to topic history
            hist.append(msg)  # Add new message to history
            if len(hist) > HISTORY_LIMIT:  # Check if history exceeds limit
                hist.popleft()  # Remove oldest message to maintain limit
            subs = list(self.topics[topic]["subs"])  # Get copy of subscribers list
        for c in subs:  # Send message to all subscribers
            try: c.sendall((msg + "\n").encode())  # Send message to subscriber
            except: self._remove_client(c)  # Remove client if send fails

    def _remove_client(self, conn):
        with self.lock:  # Acquire lock for thread safety
            for t, d in self.topics.items():  # Iterate through all topics
                if conn in d["subs"]:  # If connection is subscribed to this topic
                    d["subs"].remove(conn)  # Remove connection from subscribers

def main():
    ap = argparse.ArgumentParser()  # Create argument parser
    ap.add_argument("--host", default=DEFAULT_HOST)  # Add host argument with default
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)  # Add port argument with default
    args = ap.parse_args()  # Parse command line arguments
    srv = PubSubServer(args.host, args.port)  # Create server instance with parsed arguments
    srv.start()  # Start the server

if __name__ == "__main__":  # Only run if script is executed directly
    main()  # Call main function

"""
SUMMARY:
This code implements a TCP-based Publish-Subscribe (Pub/Sub) message broker server.

Key Features:
- Accepts TCP connections from multiple clients simultaneously using threading
- Supports two main operations: SUBSCRIBE and PUBLISH
- Maintains message history (up to 50 messages) per topic for new subscribers
- Thread-safe operations using locks to protect shared data structures
- Graceful handling of client disconnections and server shutdown

How it works:
1. Server listens on specified host/port for incoming TCP connections
2. Each client connection is handled in a separate thread
3. Clients can SUBSCRIBE to topics to receive messages
4. Clients can PUBLISH messages to topics, which are then broadcast to all subscribers
5. New subscribers receive historical messages for the topic they subscribe to
6. The server maintains a dictionary of topics, each containing subscriber connections and message history
7. When clients disconnect, they are automatically removed from all topic subscriptions

Protocol:
- SUBSCRIBE <topic> - Subscribe to receive messages from a topic
- PUBLISH <topic> <message> - Publish a message to a topic
- Server responds with "SUBSCRIBED" for successful subscriptions and "OK" for successful publishes
"""
