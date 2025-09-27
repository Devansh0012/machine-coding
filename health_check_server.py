#!/usr/bin/env python3  # Shebang to run with python3 interpreter
"""
health_check_server.py

Health Check Aggregator:
- Periodically checks endpoints (TCP/HTTP)
- Tracks history and computes health
- Exposes JSON status over HTTP
"""

# Import required modules for networking, threading, argument parsing, logging, time operations, JSON handling, and HTTP server
import socket, threading, argparse, logging, time, json, http.server, socketserver
from collections import deque  # Import deque for efficient fixed-size history storage

# Configure logging to show timestamp, level, thread name, and message
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

# Global configuration constants
CHECK_INTERVAL = 5  # Seconds between health checks
WINDOW_SIZE = 5     # Number of recent checks to keep in history
HEALTH_THRESHOLD = 0.8  # Minimum success ratio (80%) to consider service healthy

class HealthAggregator:
    def __init__(self, services):  # Initialize aggregator with list of services to monitor
        # services is list of ("tcp", host, port)
        # Create dictionary mapping service keys to service data (config, history, status)
        self.services = {self._key(s): {"conf": s, "history": deque(maxlen=WINDOW_SIZE), "last_status": None}
                         for s in services}
        self.lock = threading.Lock()  # Thread lock for safe concurrent access to service data
        self.stop_event = threading.Event()  # Event to signal when to stop monitoring

    def _key(self, s):  # ("tcp", host, port) -> "tcp:host:port"
        return f"{s[0]}:{s[1]}:{s[2]}"  # Create unique string key from service tuple

    def start(self):  # Start the health checking loop in a background thread
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):  # Main monitoring loop that runs continuously
        while not self.stop_event.is_set():  # Continue until stop event is set
            threads = []  # List to store check threads for this iteration
            for k, svc in list(self.services.items()):  # Iterate through all services
                # Create separate thread for each service check to allow parallel execution
                t = threading.Thread(target=self._check_service, args=(svc,), daemon=True)
                threads.append(t)  # Add thread to list
                t.start()  # Start the check thread
            for t in threads: t.join()  # Wait for all check threads to complete
            time.sleep(CHECK_INTERVAL)  # Wait before next round of checks

    def _check_service(self, svc):  # Check health of a single service
        proto, host, port = svc["conf"]  # Extract protocol, host, and port from service config
        ok = 0  # Initialize success flag as failure (0)
        try:
            if proto == "tcp":  # Handle TCP connection check
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create TCP socket
                sock.settimeout(2)  # Set 2 second timeout
                res = sock.connect_ex((host, port))  # Attempt connection (non-blocking)
                sock.close()  # Close socket
                ok = 1 if res == 0 else 0  # Success if connection result is 0
            elif proto == "http":  # Handle HTTP request check
                import http.client  # Import HTTP client module
                conn = http.client.HTTPConnection(host, port, timeout=2)  # Create HTTP connection with timeout
                conn.request("GET", "/")  # Send GET request to root path
                r = conn.getresponse()  # Get response
                ok = 1 if r.status == 200 else 0  # Success if status code is 200
                conn.close()  # Close connection
        except Exception:  # Catch any exceptions during check
            ok = 0  # Mark as failure

        with self.lock:  # Acquire lock for thread-safe access to shared data
            svc["history"].append(ok)  # Add check result to history deque
            ratio = sum(svc["history"]) / len(svc["history"])  # Calculate success ratio
            status = "healthy" if ratio >= HEALTH_THRESHOLD else "unhealthy"  # Determine status based on threshold
            if svc["last_status"] != status:  # Check if status changed
                logging.info("Service %s changed to %s", svc["conf"], status)  # Log status change
            svc["last_status"] = status  # Update last known status

    def get_status(self):  # Get current status of all services
        with self.lock:  # Acquire lock for thread-safe read
            result = {}  # Initialize result dictionary
            for k, svc in self.services.items():  # Iterate through all services
                # Calculate current success ratio, default to 0 if no history
                ratio = sum(svc["history"]) / len(svc["history"]) if svc["history"] else 0
                # Add service status and ratio to result
                result[k] = {"status": svc["last_status"], "success_ratio": ratio}
            return result  # Return complete status dictionary

# HTTP Handler to expose health
class HealthHandler(http.server.BaseHTTPRequestHandler):  # HTTP request handler class
    aggregator = None  # Class variable to hold reference to health aggregator

    def do_GET(self):  # Handle HTTP GET requests
        if self.path == "/health":  # Check if request is for health endpoint
            data = json.dumps(self.aggregator.get_status()).encode()  # Get status and encode as JSON bytes
            self.send_response(200)  # Send HTTP 200 OK status
            self.send_header("Content-Type", "application/json")  # Set JSON content type header
            self.send_header("Content-Length", str(len(data)))  # Set content length header
            self.end_headers()  # End headers section
            self.wfile.write(data)  # Write JSON data to response body
        else:  # For any other path
            self.send_response(404)  # Send HTTP 404 Not Found
            self.end_headers()  # End headers (no body for 404)

def run_server(host, port, aggregator):  # Start HTTP server to expose health API
    HealthHandler.aggregator = aggregator  # Set aggregator reference in handler class
    # Create threaded TCP server that can handle multiple concurrent requests
    with socketserver.ThreadingTCPServer((host, port), HealthHandler) as httpd:
        logging.info("HTTP API serving on %s:%d", host, port)  # Log server start
        httpd.serve_forever()  # Start serving requests indefinitely

def main():  # Main function to set up and run the application
    ap = argparse.ArgumentParser()  # Create argument parser
    ap.add_argument("--host", default="0.0.0.0")  # Add host argument with default
    ap.add_argument("--port", type=int, default=9005)  # Add port argument with default
    args = ap.parse_args()  # Parse command line arguments

    # Define list of services to monitor (protocol, host, port)
    services = [
        ("tcp", "google.com", 80),   # Monitor TCP connection to Google
        ("http", "example.com", 80), # Monitor HTTP response from Example.com
    ]

    agg = HealthAggregator(services)  # Create health aggregator instance
    agg.start()  # Start monitoring services in background
    run_server(args.host, args.port, agg)  # Start HTTP server (blocks here)

if __name__ == "__main__":  # Check if script is run directly (not imported)
    main()  # Call main function

"""
SUMMARY:

This health check server monitors the availability of multiple network services and provides
their status via an HTTP API. Here's how it works:

1. SERVICE MONITORING:
   - Supports TCP connection checks (tests if port is reachable)
   - Supports HTTP checks (tests if HTTP endpoint returns 200 OK)
   - Runs checks every 5 seconds in parallel threads for each service

2. HEALTH CALCULATION:
   - Maintains a rolling history of the last 5 check results per service
   - Calculates success ratio (successful checks / total checks)
   - Marks service as "healthy" if success ratio >= 80%, otherwise "unhealthy"

3. HTTP API:
   - Exposes service status at GET /health endpoint
   - Returns JSON with service status and success ratios
   - Runs on configurable host/port (default 0.0.0.0:9005)

4. THREAD SAFETY:
   - Uses locks to protect shared data access across multiple threads
   - Each service check runs in its own thread for parallelism

5. CONFIGURATION:
   - Services are hardcoded but easily modifiable
   - Check interval, history window size, and health threshold are configurable constants

The server is designed to run continuously, providing real-time health monitoring
with a simple REST API for integration with monitoring systems.
"""
