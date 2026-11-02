"""Consistent hashing with virtual nodes.

Why not ``hash(key) % N``?
    With modulo hashing, changing N (a node joins or dies) remaps almost
    every key: going from 4 to 5 nodes moves ~80% of the data.

Consistent hashing:
    Nodes and keys are hashed onto the same circle (0 .. 2**32 - 1). A key
    belongs to the first node found walking clockwise from the key's
    position. When a node joins or leaves, only the keys in the arc next to
    it move: about 1/N of the data.

Virtual nodes:
    With one point per node the arcs are very uneven, so one node can get
    far more keys than another. Each physical node is therefore placed on
    the ring many times ("node-a#0", "node-a#1", ...), which evens out the
    load and spreads a failed node's keys over ALL remaining nodes instead
    of dumping them on a single neighbour.

Replication:
    ``preference_list(key, n)`` walks clockwise and collects the first ``n``
    DISTINCT physical nodes: the key's replicas.
"""
import bisect
import hashlib
import threading


def _hash(s: str) -> int:
    # md5 is not used for security here, only for a well-spread, stable hash
    # (Python's built-in hash() is randomised per process).
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:4], "big")


class HashRing:
    def __init__(self, vnodes: int = 100):
        self.vnodes = vnodes
        self._points = []   # sorted hash positions
        self._owner = {}    # position -> physical node id
        self._nodes = set()
        self._lock = threading.Lock()

    def add_node(self, node: str) -> None:
        with self._lock:
            if node in self._nodes:
                return
            self._nodes.add(node)
            for i in range(self.vnodes):
                h = _hash(f"{node}#{i}")
                self._owner[h] = node
                bisect.insort(self._points, h)

    def remove_node(self, node: str) -> None:
        with self._lock:
            if node not in self._nodes:
                return
            self._nodes.discard(node)
            for i in range(self.vnodes):
                h = _hash(f"{node}#{i}")
                if self._owner.get(h) == node:
                    del self._owner[h]
                    idx = bisect.bisect_left(self._points, h)
                    if idx < len(self._points) and self._points[idx] == h:
                        self._points.pop(idx)

    def nodes(self):
        with self._lock:
            return set(self._nodes)

    def preference_list(self, key: str, n: int = 3):
        """First n distinct physical nodes clockwise from the key's position."""
        with self._lock:
            if not self._points:
                return []
            n = min(n, len(self._nodes))
            start = bisect.bisect(self._points, _hash(key)) % len(self._points)
            result = []
            i = start
            while len(result) < n:
                node = self._owner[self._points[i]]
                if node not in result:
                    result.append(node)
                i = (i + 1) % len(self._points)
            return result

    def get_node(self, key: str):
        prefs = self.preference_list(key, 1)
        return prefs[0] if prefs else None
