"""Load balancer / coordinator for the key-value cluster.

Clients only talk to the load balancer. For every request it:
  1. finds the key's replicas on the consistent-hash ring (3 distinct nodes),
  2. PUT: stamps the write with a new version and sends it to every replica;
     the write succeeds once a QUORUM (2 of 3) has acknowledged it,
  3. GET: asks every replica and returns the value with the highest version;
     replicas that returned an old or missing value are fixed on the spot
     ("read repair").

Failure detection (heartbeats)
    A background thread sends GET /health to every node every
    ``--heartbeat`` seconds. After ``--max-misses`` consecutive failures the
    node is declared dead and REMOVED from the ring, so its keys are served
    by the next nodes clockwise (failover to replicas).

Recovery
    When a dead node answers heartbeats again it has already rebuilt its own
    state by replaying its write-ahead log on startup. The balancer then
    adds it back to the ring and runs an anti-entropy sync: every key whose
    replicas include the node gets the newest version from the other nodes,
    covering the writes it missed while it was down.

HTTP API
    GET  /kv/<key>      PUT /kv/<key>   (body: raw value)
    GET  /cluster       node states

Run:  python -m kvstore.balancer --port 8000 --nodes 127.0.0.1:7001,127.0.0.1:7002,...
"""
import argparse
import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote

from .hashring import HashRing

REPLICAS = 3
WRITE_QUORUM = 2


class NodeClient:
    """Small HTTP client that keeps one keep-alive connection per (thread, node)."""

    def __init__(self, timeout=1.0):
        self.timeout = timeout
        self._local = threading.local()

    def _conn(self, node):
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = self._local.conns = {}
        c = conns.get(node)
        if c is None:
            host, port = node.split(":")
            c = conns[node] = http.client.HTTPConnection(host, int(port), timeout=self.timeout)
        return c

    def request(self, node, method, path, body=None):
        """Returns (status, parsed_json) or raises OSError if the node is unreachable."""
        for attempt in (1, 2):         # retry once on a stale keep-alive connection
            c = self._conn(node)
            try:
                payload = json.dumps(body).encode() if body is not None else None
                headers = {"Content-Type": "application/json"} if payload else {}
                c.request(method, path, body=payload, headers=headers)
                r = c.getresponse()
                data = r.read()
                return r.status, (json.loads(data) if data else None)
            except (OSError, http.client.HTTPException):
                c.close()
                self._local.conns.pop(node, None)
                if attempt == 2:
                    raise OSError(f"node {node} unreachable")


class Balancer:
    def __init__(self, nodes, vnodes=100, heartbeat=0.5, max_misses=3):
        self.ring = HashRing(vnodes)
        self.all_nodes = list(nodes)
        self.state = {n: "alive" for n in nodes}
        self.misses = {n: 0 for n in nodes}
        self.heartbeat = heartbeat
        self.max_misses = max_misses
        self.client = NodeClient()
        self._ver_lock = threading.Lock()
        self._last_version = 0
        for n in nodes:
            self.ring.add_node(n)

    # ---------------- versions ----------------
    def next_version(self) -> int:
        """Strictly increasing version: wall-clock nanoseconds, bumped if needed."""
        with self._ver_lock:
            self._last_version = max(self._last_version + 1, time.time_ns())
            return self._last_version

    # ---------------- reads / writes ----------------
    def put(self, key, value):
        version = self.next_version()
        replicas = self.ring.preference_list(key, REPLICAS)
        acks = 0
        for node in replicas:
            try:
                status, _ = self.client.request(node, "PUT", "/kv/" + quote(key, safe=""),
                                                {"value": value, "version": version})
                if status == 200:
                    acks += 1
            except OSError:
                pass                       # heartbeat thread will notice the node
        needed = min(WRITE_QUORUM, len(replicas))
        return acks >= needed and needed > 0, acks, replicas

    def get(self, key):
        replicas = self.ring.preference_list(key, REPLICAS)
        answers = {}                        # node -> version (0 = missing)
        best = None                         # (value, version)
        for node in replicas:
            try:
                status, body = self.client.request(node, "GET", "/kv/" + quote(key, safe=""))
            except OSError:
                continue
            if status == 200:
                answers[node] = body["version"]
                if best is None or body["version"] > best[1]:
                    best = (body["value"], body["version"])
            elif status == 404:
                answers[node] = 0
        if best is not None:
            # Read repair: bring stale or missing replicas up to date.
            for node, ver in answers.items():
                if ver < best[1]:
                    try:
                        self.client.request(node, "PUT", "/kv/" + quote(key, safe=""),
                                            {"value": best[0], "version": best[1]})
                    except OSError:
                        pass
        return best, len(answers)

    # ---------------- failure detection ----------------
    def heartbeat_loop(self):
        hb_client = NodeClient(timeout=self.heartbeat)
        while True:
            for node in self.all_nodes:
                try:
                    status, _ = hb_client.request(node, "GET", "/health")
                    healthy = status == 200
                except OSError:
                    healthy = False

                if healthy:
                    self.misses[node] = 0
                    if self.state[node] == "dead":
                        self.recover(node)
                else:
                    self.misses[node] += 1
                    if self.state[node] == "alive" and self.misses[node] >= self.max_misses:
                        self.state[node] = "dead"
                        self.ring.remove_node(node)
                        print(f"[balancer] {node} missed {self.misses[node]} heartbeats "
                              f"-> marked DEAD, removed from ring", flush=True)
            time.sleep(self.heartbeat)

    def recover(self, node):
        """Re-admit a node and copy it every write it missed (anti-entropy)."""
        self.state[node] = "recovering"
        print(f"[balancer] {node} is back -> syncing", flush=True)
        self.ring.add_node(node)

        # Latest version of every key across all live nodes.
        latest = {}
        dumps = {}
        for n in self.ring.nodes():
            try:
                status, d = self.client.request(n, "GET", "/dump")
            except OSError:
                continue
            if status != 200:
                continue
            dumps[n] = d
            for k, (v, ver) in d.items():
                if k not in latest or ver > latest[k][1]:
                    latest[k] = (v, ver)

        # Push the newest version to every replica (including the recovered
        # node) that is missing it. This also re-replicates keys that lost a
        # copy while the node was down.
        pushed = 0
        for k, (v, ver) in latest.items():
            for r in self.ring.preference_list(k, REPLICAS):
                have = dumps.get(r, {}).get(k)
                if have is None or have[1] < ver:
                    try:
                        self.client.request(r, "PUT", "/kv/" + quote(k, safe=""),
                                            {"value": v, "version": ver})
                        pushed += 1
                    except OSError:
                        pass
        self.state[node] = "alive"
        print(f"[balancer] {node} recovered, {pushed} updates pushed", flush=True)


def make_handler(lb: Balancer):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        disable_nagle_algorithm = True         # see node.py

        def log_message(self, *args):
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/cluster":
                return self._send(200, {"nodes": lb.state, "ring": sorted(lb.ring.nodes())})
            if self.path.startswith("/kv/"):
                best, responded = lb.get(unquote(self.path[4:]))
                if responded == 0:
                    return self._send(503, {"error": "no replica reachable"})
                if best is None:
                    return self._send(404, {"error": "not found"})
                return self._send(200, {"value": best[0], "version": best[1]})
            self._send(404, {"error": "bad path"})

        def do_PUT(self):
            if not self.path.startswith("/kv/"):
                return self._send(404, {"error": "bad path"})
            length = int(self.headers.get("Content-Length", 0))
            value = self.rfile.read(length).decode()
            ok, acks, replicas = lb.put(unquote(self.path[4:]), value)
            self._send(200 if ok else 503, {"ok": ok, "acks": acks, "replicas": replicas})

    return Handler


def main():
    ap = argparse.ArgumentParser(description="KV load balancer")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--nodes", required=True, help="comma-separated host:port list")
    ap.add_argument("--vnodes", type=int, default=100)
    ap.add_argument("--heartbeat", type=float, default=0.5, help="seconds between heartbeats")
    ap.add_argument("--max-misses", type=int, default=3)
    args = ap.parse_args()

    lb = Balancer(args.nodes.split(","), args.vnodes, args.heartbeat, args.max_misses)
    threading.Thread(target=lb.heartbeat_loop, daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(lb))
    server.daemon_threads = True
    print(f"balancer on :{args.port} with {len(lb.all_nodes)} nodes", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
