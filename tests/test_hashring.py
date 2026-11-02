import unittest
from collections import Counter

from kvstore.hashring import HashRing

KEYS = [f"key-{i}" for i in range(20000)]


class TestHashRing(unittest.TestCase):
    def test_even_distribution_with_virtual_nodes(self):
        ring = HashRing(vnodes=100)
        for i in range(5):
            ring.add_node(f"n{i}")
        load = Counter(ring.get_node(k) for k in KEYS)
        expected = len(KEYS) / 5
        for node, count in load.items():
            self.assertLess(abs(count - expected) / expected, 0.25,
                            f"{node} has {count} keys, expected ~{expected:.0f}")

    def test_adding_a_node_moves_about_1_over_n_keys(self):
        ring = HashRing(vnodes=100)
        for i in range(4):
            ring.add_node(f"n{i}")
        before = {k: ring.get_node(k) for k in KEYS}
        ring.add_node("n4")
        moved = sum(before[k] != ring.get_node(k) for k in KEYS)
        # Ideal is 1/5 = 20%. Modulo hashing would move ~80%.
        self.assertLess(moved / len(KEYS), 0.30)
        # and every moved key went to the NEW node, nowhere else
        self.assertTrue(all(ring.get_node(k) == "n4" for k in KEYS if before[k] != ring.get_node(k)))

    def test_removing_a_node_only_moves_its_keys(self):
        ring = HashRing(vnodes=100)
        for i in range(5):
            ring.add_node(f"n{i}")
        before = {k: ring.get_node(k) for k in KEYS}
        ring.remove_node("n2")
        for k in KEYS:
            if before[k] != "n2":
                self.assertEqual(before[k], ring.get_node(k))

    def test_preference_list_has_distinct_nodes(self):
        ring = HashRing(vnodes=50)
        for i in range(5):
            ring.add_node(f"n{i}")
        for k in KEYS[:2000]:
            prefs = ring.preference_list(k, 3)
            self.assertEqual(len(prefs), 3)
            self.assertEqual(len(set(prefs)), 3)
            self.assertEqual(prefs[0], ring.get_node(k))

    def test_preference_list_capped_by_cluster_size(self):
        ring = HashRing()
        ring.add_node("only")
        self.assertEqual(ring.preference_list("x", 3), ["only"])


if __name__ == "__main__":
    unittest.main()
