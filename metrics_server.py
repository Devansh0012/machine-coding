#!/usr/bin/env python3
"""
metrics_server.py

Metrics Collection Service over TCP.

Protocol:
    RECORD <metric_name> <value> <timestamp> - Record a metric
    GET <metric_name> <start_time> <end_time> - Get metrics in time range
    AGGREGATE <metric_name> <function> <window_seconds> - Get aggregated data
    LIST_METRICS - List all available metrics
"""

import socket
import threading
import argparse
import logging
import time
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import statistics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")

@dataclass
class MetricPoint:
    timestamp: float
    value: float

class MetricsStore:
    def __init__(self, retention_hours=24):
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque())
        self.retention_seconds = retention_hours * 3600
        self.lock = threading.Lock()
        
        # Start cleanup thread
        threading.Thread(target=self._cleanup_old_metrics, daemon=True).start()

    def record_metric(self, metric_name: str, value: float, timestamp: float = None):
        if timestamp is None:
            timestamp = time.time()
        
        point = MetricPoint(timestamp, value)
        
        with self.lock:
            self.metrics[metric_name].append(point)
            logging.debug("Recorded %s = %f at %f", metric_name, value, timestamp)

    def get_metrics(self, metric_name: str, start_time: float, end_time: float) -> List[MetricPoint]:
        with self.lock:
            if metric_name not in self.metrics:
                return []
            
            points = []
            for point in self.metrics[metric_name]:
                if start_time <= point.timestamp <= end_time:
                    points.append(point)
            
            return sorted(points, key=lambda p: p.timestamp)

    def aggregate_metrics(self, metric_name: str, function: str, window_seconds: int) -> dict:
        now = time.time()
        start_time = now - window_seconds
        
        points = self.get_metrics(metric_name, start_time, now)
        if not points:
            return {"error": "No data available"}
        
        values = [p.value for p in points]
        
        result = {
            "metric_name": metric_name,
            "window_seconds": window_seconds,
            "count": len(values),
            "start_time": start_time,
            "end_time": now
        }
        
        try:
            if function == "avg" or function == "mean":
                result["value"] = statistics.mean(values)
            elif function == "sum":
                result["value"] = sum(values)
            elif function == "min":
                result["value"] = min(values)
            elif function == "max":
                result["value"] = max(values)
            elif function == "median":
                result["value"] = statistics.median(values)
            elif function == "p95":
                result["value"] = self._percentile(values, 0.95)
            elif function == "p99":
                result["value"] = self._percentile(values, 0.99)
            elif function == "stddev":
                result["value"] = statistics.stdev(values) if len(values) > 1 else 0
            else:
                result["error"] = f"Unknown function: {function}"
        except Exception as e:
            result["error"] = str(e)
        
        return result

    def list_metrics(self) -> List[str]:
        with self.lock:
            return list(self.metrics.keys())

    def get_metric_stats(self, metric_name: str) -> dict:
        with self.lock:
            if metric_name not in self.metrics:
                return {"error": "Metric not found"}
            
            points = list(self.metrics[metric_name])
            if not points:
                return {"count": 0}
            
            values = [p.value for p in points]
            return {
                "count": len(points),
                "first_timestamp": points[0].timestamp,
                "last_timestamp": points[-1].timestamp,
                "min_value": min(values),
                "max_value": max(values),
                "avg_value": statistics.mean(values)
            }

    def _percentile(self, values: List[float], percentile: float) -> float:
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        if index >= len(sorted_values):
            index = len(sorted_values) - 1
        return sorted_values[index]

    def _cleanup_old_metrics(self):
        while True:
            cutoff_time = time.time() - self.retention_seconds
            
            with self.lock:
                for metric_name, points in self.metrics.items():
                    # Remove old points
                    while points and points[0].timestamp < cutoff_time:
                        points.popleft()
                
                # Remove empty metrics
                empty_metrics = [name for name, points in self.metrics.items() if not points]
                for name in empty_metrics:
                    del self.metrics[name]
            
            time.sleep(300)  # Cleanup every 5 minutes

class MetricsServer:
    def __init__(self, host="0.0.0.0", port=9014):
        self.host = host
        self.port = port
        self.store = MetricsStore()
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(100)
        logging.info("MetricsServer listening on %s:%d", self.host, self.port)
        
        try:
            while True:
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            if self.sock:
                self.sock.close()

    def _handle_client(self, conn, addr):
        logging.info("Client connected %s", addr)
        try:
            f = conn.makefile("rwb")
            while True:
                line = f.readline()
                if not line:
                    break
                
                line = line.decode().strip()
                if not line:
                    continue
                
                parts = line.split()
                cmd = parts[0].upper()
                
                if cmd == "RECORD" and len(parts) >= 3:
                    metric_name = parts[1]
                    try:
                        value = float(parts[2])
                        timestamp = float(parts[3]) if len(parts) > 3 else None
                        self.store.record_metric(metric_name, value, timestamp)
                        conn.sendall(b"OK\n")
                    except ValueError:
                        conn.sendall(b"ERROR invalid value or timestamp\n")
                
                elif cmd == "GET" and len(parts) == 4:
                    metric_name = parts[1]
                    try:
                        start_time = float(parts[2])
                        end_time = float(parts[3])
                        points = self.store.get_metrics(metric_name, start_time, end_time)
                        
                        result = {
                            "metric_name": metric_name,
                            "points": [{"timestamp": p.timestamp, "value": p.value} for p in points]
                        }
                        conn.sendall((json.dumps(result) + "\n").encode())
                    except ValueError:
                        conn.sendall(b"ERROR invalid timestamp\n")
                
                elif cmd == "AGGREGATE" and len(parts) == 4:
                    metric_name = parts[1]
                    function = parts[2].lower()
                    try:
                        window_seconds = int(parts[3])
                        result = self.store.aggregate_metrics(metric_name, function, window_seconds)
                        conn.sendall((json.dumps(result) + "\n").encode())
                    except ValueError:
                        conn.sendall(b"ERROR invalid window\n")
                
                elif cmd == "LIST_METRICS":
                    metrics = self.store.list_metrics()
                    result = {"metrics": metrics}
                    conn.sendall((json.dumps(result) + "\n").encode())
                
                elif cmd == "STATS" and len(parts) == 2:
                    metric_name = parts[1]
                    stats = self.store.get_metric_stats(metric_name)
                    conn.sendall((json.dumps(stats) + "\n").encode())
                
                else:
                    conn.sendall(b"ERROR unknown command\n")
                    
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            conn.close()
            logging.info("Client disconnected %s", addr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9014)
    args = ap.parse_args()
    
    srv = MetricsServer(args.host, args.port)
    srv.start()

if __name__ == "__main__":
    main()