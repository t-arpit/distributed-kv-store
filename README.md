# Distributed key-value store with load balancing

A sharded, 3-way replicated key-value store in Python. A load balancer routes requests using **consistent hashing with virtual nodes**, detects crashed nodes with **heartbeats**, and keeps data safe with a **write-ahead log**. It uses only the Python standard library, so there's nothing to install.

```
                        ┌──────────────┐
  clients ── HTTP ────▶ │ load balancer│  hash ring + heartbeats + quorum writes
                        └──────┬───────┘
          ┌──────────┬─────────┼─────────┬──────────┐
          ▼          ▼         ▼         ▼          ▼
       node 1     node 2    node 3    node 4     node 5     each: dict + WAL on disk
```

## Quick start

```bash
python3 -m kvstore.cluster --nodes 5        # starts 5 nodes + balancer, prints the URL
curl -X PUT -d hello http://127.0.0.1:<port>/kv/greeting
curl http://127.0.0.1:<port>/kv/greeting     # {"value": "hello", "version": ...}
curl http://127.0.0.1:<port>/cluster         # node states and ring members
```

Or run the pieces yourself:
```bash
python3 -m kvstore.node --port 7001 --data-dir data/n1      # x N
python3 -m kvstore.balancer --port 8000 --nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
```

## How it works

### Sharding with consistent hashing (`kvstore/hashring.py`)
- Nodes and keys are hashed onto the same ring (0 … 2³²−1). A key belongs to the first node found walking clockwise from it.
- **Why not `hash(key) % N`?** Changing N remaps almost every key: going from 4 to 5 nodes moves about 80% of the data. With a ring, only about **1/N** of the keys move, and the tests check this.
- **Virtual nodes:** each physical node is placed on the ring 100 times. This evens out the load, and when a node dies its keys spread over *all* the remaining nodes instead of landing on one neighbour.

### Replication and quorums (`kvstore/balancer.py`)
- Each key is stored on the **first 3 distinct nodes** clockwise (its *preference list*).
- **PUT:** the balancer stamps the write with a strictly increasing **version** and sends it to all 3 replicas. The write succeeds once a **quorum of 2** has acknowledged it.
- **GET:** the balancer asks all replicas and returns the **highest version**. Replicas holding an older or missing copy are fixed straight away (**read repair**).
- Nodes reject writes with an older version, so writes are idempotent and safe to retry or replay.

### Failure detection (heartbeats)
- A background thread sends `GET /health` to every node every 0.5 s.
- After **3 missed heartbeats** the node is marked dead and **removed from the ring**. Its keys are then served by the next nodes clockwise, which already hold replicas (**failover**).

### Durability and recovery (`kvstore/node.py`)
- **Write-ahead log:** every write is appended to `wal.log` and `fsync`ed **before** it's applied in memory. A crash loses the in-memory dict but not the log.
- On restart a node **replays its WAL** and rebuilds exactly the state it had before the crash. A half-written last line from a crash is skipped; it was never acknowledged.
- When a dead node answers heartbeats again, the balancer adds it back to the ring and runs an **anti-entropy sync**, pushing it every write it missed while it was down.

## Tests

```bash
python3 -m unittest discover -s tests -t . -v
```

- `tests/test_hashring.py` checks even distribution, that adding a node moves only about 1/N of the keys (all of them to the new node), that removal moves only the removed node's keys, and that preference lists contain distinct nodes.
- `tests/test_cluster.py` runs **fault injection on a real multi-process cluster**, killing nodes with `SIGKILL` (a real crash):
  - each key is stored exactly 3 times, spread over all nodes
  - one node crashes: zero data loss, and writes keep working
  - two nodes crash at once: zero data loss
  - a crashed node restarts, replays its WAL, catches up on missed writes, and then serves data while two *other* nodes are killed
  - 8 concurrent clients write while a node is killed mid-traffic, and every acknowledged write survives

## Benchmark

```bash
python3 -m bench.benchmark --nodes 3 5 10 --clients 1 16 --seconds 3
```

A sample run on a laptop-class machine (50/50 PUT/GET, 100-byte values, no fsync):

```
nodes clients     ops/s   PUT p50   PUT p99   GET p50   GET p99   (latencies in ms)
    3       1       514      2.06      3.57      1.70      3.84
    3      16       668     23.36     53.32     18.02     43.44
    5       1       390      2.54      5.98      2.19      6.25
   10      16       630     20.22     59.99     17.95     62.23
```

Throughput stays flat as nodes are added because the **single Python load balancer is the bottleneck** (the GIL, plus 3 replica round-trips per request). Every request still costs the same 3 replica calls, so adding nodes spreads the *storage* but not the coordination work. Ways to scale it further: several balancers, or clients that read the ring and talk to replicas directly (as in Cassandra and Dynamo), plus asynchronous I/O.

## Layout

```
kvstore/hashring.py    consistent hashing with virtual nodes
kvstore/node.py        storage node: dict + write-ahead log + HTTP API
kvstore/balancer.py    routing, quorum writes, read repair, heartbeats, recovery
kvstore/client.py      small client
kvstore/cluster.py     start/kill local clusters (tests, benchmark, demo)
tests/                 unit and fault-injection tests
bench/benchmark.py     throughput and latency benchmark
```
