"""A storage node: an in-memory key-value store made durable by a write-ahead log.

Write-ahead logging (WAL)
    Every write is first APPENDED to a log file on disk and fsync'ed, and
    only then applied to the in-memory dict. If the process crashes, the
    dict is lost but the log is not. On restart the node REPLAYS the log
    from the beginning and ends up in exactly the state it had before the
    crash. Appending is sequential I/O, so it is cheap.

Versions
    Each value carries a version number assigned by the load balancer. A
    node only accepts a write if its version is newer than what it already
    has. This makes writes idempotent (safe to retry or replay) and lets
    replicas that missed writes catch up without overwriting newer data.

HTTP API (JSON)
    GET  /health            -> {"status": "ok", "node": ...}
    GET  /kv/<key>          -> {"value": ..., "version": ...}  or 404
    PUT  /kv/<key>          body {"value": ..., "version": ...}
    GET  /dump              -> {key: [value, version], ...}   (used for recovery)
    GET  /stats             -> counters

Run:  python -m kvstore.node --port 7001 --data-dir data/node1
"""
import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote


class Store:
    def __init__(self, wal_path: str, fsync: bool = True):
        self.wal_path = wal_path
        self.fsync = fsync
        self.data = {}           # key -> (value, version)
        self.lock = threading.Lock()
        self.replayed = 0
        self.writes = 0
        self._replay()
        self._wal = open(wal_path, "a", encoding="utf-8")

    def _replay(self):
        """Rebuild the in-memory state by re-applying every logged write in order."""
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    # A crash in the middle of a write can leave a torn last
                    # line. It was never acknowledged, so it is safe to skip.
                    continue
                self._apply(rec["k"], rec["v"], rec["ver"])
                self.replayed += 1

    def _apply(self, key, value, version) -> bool:
        cur = self.data.get(key)
        if cur is not None and cur[1] >= version:
            return False                      # stale or duplicate write
        self.data[key] = (value, version)
        return True

    def put(self, key, value, version) -> bool:
        with self.lock:
            cur = self.data.get(key)
            if cur is not None and cur[1] >= version:
                return False
            # 1) log first ...
            self._wal.write(json.dumps({"k": key, "v": value, "ver": version}) + "\n")
            self._wal.flush()
            if self.fsync:
                os.fsync(self._wal.fileno())
            # 2) ... then apply
            self._apply(key, value, version)
            self.writes += 1
            return True

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def dump(self):
        with self.lock:
            return {k: [v, ver] for k, (v, ver) in self.data.items()}


def make_handler(store: Store, node_id: str):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"          # keep-alive connections
        # Headers and body are written separately; without TCP_NODELAY, Nagle's
        # algorithm + delayed ACKs add ~40 ms to every response.
        disable_nagle_algorithm = True

        def log_message(self, *args):          # keep the console quiet
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                return self._send(200, {"status": "ok", "node": node_id})
            if self.path == "/dump":
                return self._send(200, store.dump())
            if self.path == "/stats":
                return self._send(200, {"node": node_id, "keys": len(store.data),
                                        "writes": store.writes, "replayed": store.replayed})
            if self.path.startswith("/kv/"):
                item = store.get(unquote(self.path[4:]))
                if item is None:
                    return self._send(404, {"error": "not found"})
                return self._send(200, {"value": item[0], "version": item[1]})
            self._send(404, {"error": "bad path"})

        def do_PUT(self):
            if not self.path.startswith("/kv/"):
                return self._send(404, {"error": "bad path"})
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            applied = store.put(unquote(self.path[4:]), body["value"], int(body["version"]))
            self._send(200, {"ok": True, "applied": applied})

    return Handler


def main():
    ap = argparse.ArgumentParser(description="KV storage node")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--no-fsync", action="store_true", help="faster, less durable")
    args = ap.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    node_id = f"{args.host}:{args.port}"
    store = Store(os.path.join(args.data_dir, "wal.log"), fsync=not args.no_fsync)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(store, node_id))
    server.daemon_threads = True
    print(f"node {node_id} up, replayed {store.replayed} WAL entries, "
          f"{len(store.data)} keys", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
