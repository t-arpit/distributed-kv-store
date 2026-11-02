"""Benchmark read/write throughput and latency for different cluster sizes and loads.

    python -m bench.benchmark                         # default sweep
    python -m bench.benchmark --nodes 3 5 10 --clients 4 16 --seconds 3

For every (nodes, clients) combination a fresh local cluster is started and
`clients` threads issue a 50/50 mix of PUTs and GETs for `seconds` seconds.
"""
import argparse
import random
import statistics
import threading
import time

from kvstore.client import KVClient
from kvstore.cluster import LocalCluster


def run(n_nodes, n_clients, seconds, fsync, keyspace=10000):
    with LocalCluster(n_nodes, fsync=fsync) as cluster:
        # preload so GETs hit real data
        c = KVClient(cluster.url)
        for i in range(500):
            c.put(f"key{i}", "x" * 100)
        c.close()

        lat = {"PUT": [], "GET": []}
        lock = threading.Lock()
        stop = time.time() + seconds

        def worker(seed):
            rnd = random.Random(seed)
            cl = KVClient(cluster.url)
            local = {"PUT": [], "GET": []}
            while time.time() < stop:
                k = f"key{rnd.randrange(keyspace)}"
                op = "PUT" if rnd.random() < 0.5 else "GET"
                t0 = time.perf_counter()
                cl.put(k, "v" * 100) if op == "PUT" else cl.get(k)
                local[op].append((time.perf_counter() - t0) * 1000)
            cl.close()
            with lock:
                for op in local:
                    lat[op].extend(local[op])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def pct(xs, p):
        return statistics.quantiles(xs, n=100)[p - 1] if len(xs) >= 2 else float("nan")

    total = len(lat["PUT"]) + len(lat["GET"])
    return {
        "ops_s": total / seconds,
        "put_p50": pct(lat["PUT"], 50), "put_p99": pct(lat["PUT"], 99),
        "get_p50": pct(lat["GET"], 50), "get_p99": pct(lat["GET"], 99),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, nargs="+", default=[3, 5, 8, 10])
    ap.add_argument("--clients", type=int, nargs="+", default=[1, 8, 32])
    ap.add_argument("--seconds", type=float, default=3.0)
    ap.add_argument("--fsync", action="store_true", help="fsync every WAL write")
    args = ap.parse_args()

    print(f"{'nodes':>5} {'clients':>7} {'ops/s':>9} {'PUT p50':>9} {'PUT p99':>9} "
          f"{'GET p50':>9} {'GET p99':>9}   (latencies in ms)")
    for n in args.nodes:
        for c in args.clients:
            r = run(n, c, args.seconds, args.fsync)
            print(f"{n:>5} {c:>7} {r['ops_s']:>9.0f} {r['put_p50']:>9.2f} {r['put_p99']:>9.2f} "
                  f"{r['get_p50']:>9.2f} {r['get_p99']:>9.2f}", flush=True)


if __name__ == "__main__":
    main()
