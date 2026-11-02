"""End-to-end fault-injection tests on a real local cluster (separate processes).

Nodes are killed with SIGKILL (a real crash, no clean shutdown) while clients
keep reading and writing, and we check that no acknowledged write is ever lost.
"""
import json
import threading
import time
import unittest
import urllib.request

from kvstore.client import KVClient
from kvstore.cluster import LocalCluster

HEARTBEAT = 0.2
MAX_MISSES = 3
DETECT = HEARTBEAT * (MAX_MISSES + 3)       # generous time to detect a failure


def wait_until(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def node_stats(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/stats") as r:
        return json.loads(r.read())


class TestCluster(unittest.TestCase):
    def setUp(self):
        # fsync off for speed: a SIGKILLed process still leaves its writes in the OS cache
        self.cluster = LocalCluster(5, heartbeat=HEARTBEAT, max_misses=MAX_MISSES,
                                    fsync=False).start()
        self.kv = KVClient(self.cluster.url)

    def tearDown(self):
        self.kv.close()
        self.cluster.stop()

    def write_many(self, n, prefix="k"):
        data = {f"{prefix}{i}": f"value-{prefix}-{i}" for i in range(n)}
        for k, v in data.items():
            self.assertTrue(self.kv.put(k, v), f"write {k} not acknowledged")
        return data

    def assert_all_readable(self, data):
        missing = [k for k, v in data.items() if self.kv.get(k) != v]
        self.assertEqual(missing, [], f"{len(missing)} keys lost or wrong")

    def node_state(self, port):
        return self.kv.cluster()["nodes"][f"127.0.0.1:{port}"]

    # ------------------------------------------------------------------
    def test_basic_put_get_overwrite(self):
        self.assertTrue(self.kv.put("a", "1"))
        self.assertEqual(self.kv.get("a"), "1")
        self.assertTrue(self.kv.put("a", "2"))
        self.assertEqual(self.kv.get("a"), "2")
        self.assertIsNone(self.kv.get("missing"))
        self.assertTrue(self.kv.put("key with spaces/and?symbols", "ok"))
        self.assertEqual(self.kv.get("key with spaces/and?symbols"), "ok")

    def test_data_is_sharded_and_replicated_3_ways(self):
        self.write_many(300)
        total = sum(node_stats(p)["keys"] for p in self.cluster.ports)
        self.assertEqual(total, 300 * 3)                      # 3 copies of every key
        for p in self.cluster.ports:                          # spread over all nodes
            self.assertGreater(node_stats(p)["keys"], 0)

    def test_single_node_crash_no_data_loss(self):
        data = self.write_many(300)
        victim = self.cluster.ports[0]
        self.cluster.kill_node(victim)
        self.assertTrue(wait_until(lambda: self.node_state(victim) == "dead", DETECT * 5))
        self.assertNotIn(f"127.0.0.1:{victim}", self.kv.cluster()["ring"])
        self.assert_all_readable(data)
        # writes keep working while the node is down
        self.assert_all_readable(self.write_many(100, prefix="during"))

    def test_two_node_crash_no_data_loss(self):
        data = self.write_many(300)
        a, b = self.cluster.ports[:2]
        self.cluster.kill_node(a)
        self.cluster.kill_node(b)
        self.assertTrue(wait_until(lambda: self.node_state(a) == "dead" and
                                   self.node_state(b) == "dead", DETECT * 5))
        self.assert_all_readable(data)

    def test_restarted_node_replays_wal_and_catches_up(self):
        before = self.write_many(200, prefix="before")
        victim = self.cluster.ports[1]
        keys_before_crash = node_stats(victim)["keys"]

        self.cluster.kill_node(victim)
        self.assertTrue(wait_until(lambda: self.node_state(victim) == "dead", DETECT * 5))
        during = self.write_many(200, prefix="during")      # missed by the victim

        self.cluster.start_node(victim)                     # restart, same data dir
        # On startup it rebuilt its state by replaying its own WAL ...
        self.assertEqual(node_stats(victim)["replayed"], keys_before_crash)
        # ... then the balancer re-admits it and syncs the writes it missed.
        self.assertTrue(wait_until(lambda: self.node_state(victim) == "alive", DETECT * 5))
        self.assertGreater(node_stats(victim)["keys"], keys_before_crash)  # caught up

        # Now crash two OTHER nodes: data must survive using the recovered node.
        for p in [p for p in self.cluster.ports if p != victim][:2]:
            self.cluster.kill_node(p)
        time.sleep(DETECT)
        self.assert_all_readable(before)
        self.assert_all_readable(during)

    def test_concurrent_clients_during_crash(self):
        acked, lock, errors = {}, threading.Lock(), []

        def worker(wid):
            c = KVClient(self.cluster.url)
            for i in range(150):
                k, v = f"w{wid}-{i}", f"v{i}"
                try:
                    if c.put(k, v):
                        with lock:
                            acked[k] = v
                except Exception as e:          # pragma: no cover
                    errors.append(e)
            c.close()

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
        for t in threads:
            t.start()
        time.sleep(0.3)
        self.cluster.kill_node(self.cluster.ports[2])        # crash mid-traffic
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        time.sleep(DETECT)
        self.assertGreater(len(acked), 1000)
        self.assert_all_readable(acked)                      # every acknowledged write survives


if __name__ == "__main__":
    unittest.main()
