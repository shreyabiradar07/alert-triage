"""A small shop to break on purpose: six services, one Redis, one load generator.

    frontend -> checkout -> payments
                         -> cart -> redis-cart
             -> search -> search-index

Two more containers make noise by themselves, with nobody injecting anything:

    batch-reports       a job that runs a core hot for two minutes out of every ten
    checkout-staging    a staging copy that breaks itself (60% errors) every twelve minutes

Every service is this file with a different ROLE. Each serves GET / (does its work and calls
its dependencies), GET /metrics (Prometheus), and POST /chaos to inject a fault:

    {"error_rate": 0.4}      fail this share of requests with a 500
    {"latency": 2.5}         add this many seconds to every request
    {"cpu": true}            spin a core
    {"leak_mb": 20}          hold on to this much more memory every second
    {}                       back to healthy

A failed dependency turns into a 502, so a fault deep in the chain shows up in its callers.
"""

import json
import os
import random
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

ROLE = os.environ.get("ROLE", "frontend")
PORT = 8000
STAGING = bool(os.environ.get("STAGING"))
CALLS = [] if STAGING else {"frontend": ["checkout", "search"], "checkout": ["payments", "cart"],
                            "search": ["search-index"]}.get(ROLE, [])
REQUESTS = Counter("http_requests_total", "Requests served", ["code"])
DURATION = Histogram("http_request_duration_seconds", "Time to serve a request",
                     buckets=(.05, .1, .25, .5, .75, 1, 1.25, 1.5, 2, 2.5, 5, 10))
fault = {}  # set through POST /chaos
own = {}    # what the service does to itself on a timer (the batch job, the staging copy)
leaked = []


def setting(key, default=0):
    return fault.get(key, own.get(key, default))


def timetable(first: float, every: float, lasts: float, **settings):
    """Switch `settings` on for `lasts` seconds out of every `every`, starting after `first`."""
    time.sleep(first)
    while True:
        own.update(settings)
        time.sleep(lasts)
        own.clear()
        time.sleep(every - lasts)


def self_traffic():
    """The staging copy has no callers, so it sends itself requests to have something to measure."""
    while True:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/", timeout=4) as resp:
                resp.read()
        except Exception:
            pass
        time.sleep(0.2)


def call(service: str):
    with urllib.request.urlopen(f"http://{service}:{PORT}/", timeout=4) as resp:
        resp.read()


def redis_incr():
    with socket.create_connection(("redis-cart", 6379), timeout=2) as s:
        s.sendall(b"INCR carts\r\n")
        if not s.recv(64).startswith(b":"):
            raise RuntimeError("unexpected reply from redis")


def background():
    """The CPU and memory faults, applied once a second while they are switched on."""
    while True:
        if setting("cpu"):
            end = time.time() + 0.95
            while time.time() < end:
                pass
        else:
            time.sleep(1)
        if setting("leak_mb"):
            leaked.append(bytearray(int(setting("leak_mb")) * 1024 * 1024))
        elif leaked:
            leaked.clear()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code: int, body: bytes = b"ok", kind: str = "text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/metrics":
            return self.reply(200, generate_latest(), CONTENT_TYPE_LATEST)
        start, code = time.time(), 200
        try:
            time.sleep(random.uniform(0.005, 0.03) + float(setting("latency")))
            if random.random() < float(setting("error_rate")):
                code = 500
            else:
                for service in CALLS:
                    call(service)
                if ROLE == "cart":
                    redis_incr()
        except Exception:
            code = 502
        REQUESTS.labels(str(code)).inc()
        DURATION.observe(time.time() - start)
        self.reply(code)

    def do_POST(self):
        if self.path != "/chaos":
            return self.reply(404)
        new = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        fault.clear()
        fault.update(new)
        self.reply(200, json.dumps(fault).encode(), "application/json")

    def log_message(self, *args):
        pass


def loadgen():
    def worker():
        while True:
            try:
                call("frontend")
            except Exception:
                pass
            time.sleep(random.uniform(0.1, 0.3))
    for _ in range(4):
        threading.Thread(target=worker, daemon=True).start()
    threading.Event().wait()


if __name__ == "__main__":
    if ROLE == "loadgen":
        loadgen()
    threading.Thread(target=background, daemon=True).start()
    if ROLE == "batch-reports":
        threading.Thread(target=timetable, args=(120, 600, 110), kwargs={"cpu": True}, daemon=True).start()
    if STAGING:
        threading.Thread(target=self_traffic, daemon=True).start()
        threading.Thread(target=timetable, args=(300, 720, 150), kwargs={"error_rate": 0.6}, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
