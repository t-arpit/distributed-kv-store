"""Start, stop and crash processes of a local cluster (used by tests, benchmark and demo).

    python -m kvstore.cluster --nodes 5      # start 5 nodes + balancer, Ctrl-C to stop
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_http(url, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=0.5):
                return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError(f"{url} did not come up")


class LocalCluster:
    def __init__(self, n_nodes=5, data_dir=None, fsync=True, heartbeat=0.2, max_misses=3,
                 quiet=True):
        self.n = n_nodes
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="kv-")
        self.fsync = fsync
        self.heartbeat = heartbeat
        self.max_misses = max_misses
        self.quiet = quiet
        self.ports = [free_port() for _ in range(n_nodes)]
        self.lb_port = free_port()
        self.nodes = {}          # port -> Popen
        self.lb = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.lb_port}"

    def _out(self):
        return subprocess.DEVNULL if self.quiet else None

    def start_node(self, port):
        cmd = [sys.executable, "-m", "kvstore.node", "--port", str(port),
               "--data-dir", os.path.join(self.data_dir, f"node-{port}")]
        if not self.fsync:
            cmd.append("--no-fsync")
        self.nodes[port] = subprocess.Popen(cmd, cwd=ROOT, stdout=self._out(), stderr=self._out())
        wait_http(f"http://127.0.0.1:{port}/health")

    def kill_node(self, port):
        """Simulate a crash: SIGKILL gives the process no chance to clean up."""
        p = self.nodes.pop(port)
        p.send_signal(signal.SIGKILL)
        p.wait()

    def start(self):
        for p in self.ports:
            self.start_node(p)
        nodes = ",".join(f"127.0.0.1:{p}" for p in self.ports)
        self.lb = subprocess.Popen(
            [sys.executable, "-m", "kvstore.balancer", "--port", str(self.lb_port),
             "--nodes", nodes, "--heartbeat", str(self.heartbeat),
             "--max-misses", str(self.max_misses)],
            cwd=ROOT, stdout=self._out(), stderr=self._out())
        wait_http(self.url + "/cluster")
        return self

    def stop(self):
        for p in list(self.nodes.values()):
            p.terminate()
            p.wait()
        self.nodes.clear()
        if self.lb:
            self.lb.terminate()
            self.lb.wait()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5)
    args = ap.parse_args()
    c = LocalCluster(args.nodes, quiet=False).start()
    print(f"\ncluster ready: balancer at {c.url}, data in {c.data_dir}")
    print("try:  curl -X PUT -d hello " + c.url + "/kv/greeting")
    print("      curl " + c.url + "/kv/greeting")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        c.stop()


if __name__ == "__main__":
    main()
