#!/usr/bin/env python3  # Shebang line to specify Python 3 interpreter
"""
http_file_server.py
Minimal HTTP/1.1 server with Range support
"""
import socket, threading, argparse, logging, os  # Import required modules for networking, threading, CLI args, logging, and file operations

DEFAULT_HOST = "0.0.0.0"  # Default host to bind to (all interfaces)
DEFAULT_PORT = 9004       # Default port number
ROOT_DIR = "."           # Default root directory for serving files

# Configure logging with timestamp, level, thread name, and message
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

class HTTPServer:  # Main HTTP server class
    def __init__(self, host, port, root):  # Constructor to initialize server parameters
        self.host, self.port, self.root = host, port, root  # Store host, port, and root directory

    def start(self):  # Method to start the HTTP server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow socket reuse
        sock.bind((self.host, self.port))  # Bind socket to host and port
        sock.listen(50)  # Listen for up to 50 connections
        logging.info("HTTP server on %s:%d", self.host, self.port)  # Log server start
        while True:  # Infinite loop to accept connections
            conn, addr = sock.accept()  # Accept incoming connection
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()  # Handle each connection in separate thread

    def _handle(self, conn, addr):  # Method to handle individual client connections
        try:  # Try block for error handling
            req = conn.recv(4096).decode(errors="ignore")  # Read request data and decode to string
            if not req: return  # Return if no request data
            line, *hdrs = req.split("\r\n")  # Split request into first line and headers
            method, path, _ = line.split()  # Parse HTTP method, path, and version
            if method != "GET":  # Check if method is GET
                conn.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")  # Send 405 error for non-GET methods
                return  # Exit method
            filepath = os.path.join(self.root, path.lstrip("/"))  # Construct file path from root and requested path
            if not os.path.isfile(filepath):  # Check if file exists
                conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")  # Send 404 error if file not found
                return  # Exit method
            size = os.path.getsize(filepath)  # Get file size
            # Parse Range header
            start, end = 0, size - 1  # Default range is entire file
            for h in hdrs:  # Iterate through headers
                if h.lower().startswith("range:"):  # Check for Range header
                    _, val = h.split(":", 1)  # Split header name and value
                    rng = val.strip().split("=")[1]  # Extract range value after "bytes="
                    start_s, end_s = rng.split("-")  # Split start and end positions
                    if start_s: start = int(start_s)  # Set start position if specified
                    if end_s: end = int(end_s)  # Set end position if specified
            length = end - start + 1  # Calculate content length
            with open(filepath, "rb") as f:  # Open file in binary read mode
                f.seek(start)  # Seek to start position
                data = f.read(length)  # Read requested amount of data
            status = b"206 Partial Content" if start > 0 or end < size-1 else b"200 OK"  # Set status based on range request
            resp = (  # Build HTTP response
                b"HTTP/1.1 " + status + b"\r\n" +  # Status line
                f"Content-Length: {len(data)}\r\n".encode() +  # Content length header
                f"Content-Range: bytes {start}-{end}/{size}\r\n".encode() +  # Content range header
                b"Connection: close\r\n\r\n"  # Connection close header and end of headers
            )
            conn.sendall(resp + data)  # Send response headers and file data
        except Exception as e:  # Catch any exceptions
            logging.exception("Error handling %s", addr)  # Log error with client address
        finally:  # Always execute cleanup
            conn.close()  # Close connection

def main():  # Main function
    ap = argparse.ArgumentParser()  # Create argument parser
    ap.add_argument("--host", default=DEFAULT_HOST)  # Add host argument
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)  # Add port argument
    ap.add_argument("--root", default=ROOT_DIR)  # Add root directory argument
    args = ap.parse_args()  # Parse command line arguments
    HTTPServer(args.host, args.port, args.root).start()  # Create and start HTTP server

if __name__ == "__main__":  # Check if script is run directly
    main()  # Call main function

"""
SUMMARY:
This code implements a minimal HTTP/1.1 file server with Range request support.
Key features:
- Serves files from a specified root directory
- Supports HTTP Range requests for partial content delivery (useful for streaming/resumable downloads)
- Multi-threaded to handle multiple concurrent connections
- Command-line interface for configuring host, port, and root directory
- Returns appropriate HTTP status codes (200 OK, 206 Partial Content, 404 Not Found, 405 Method Not Allowed)
- Includes proper error handling and logging

The server binds to a socket, listens for connections, and spawns a new thread for each client.
Each thread parses the HTTP request, validates it's a GET request, checks if the file exists,
processes any Range headers for partial content requests, and sends back the appropriate
HTTP response with the requested file data.
"""
