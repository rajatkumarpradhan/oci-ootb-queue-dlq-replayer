"""Visibility timeout, redelivery, DLQ threshold and replay semantics."""
import unittest

from oci_queue_dlq_replayer import OciQueue, replay_dlq


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_queue(**kw):
    clock = FakeClock()
    kw.setdefault("clock", clock)
    return OciQueue("q", **kw), clock


class EnqueueDequeueTests(unittest.TestCase):
    def test_enqueue_rejects_empty_body(self):
        q, _ = make_queue()
        for bad in ("", None, 42):
            with self.assertRaises(ValueError):
                q.enqueue(bad)

    def test_message_ids_are_unique(self):
        q, _ = make_queue()
        ids = {q.enqueue(f"m{i}").message_id for i in range(5)}
        self.assertEqual(len(ids), 5)

    def test_constructor_validation(self):
        with self.assertRaises(ValueError):
            OciQueue("q", max_deliveries=0)
        with self.assertRaises(ValueError):
            OciQueue("q", visibility_timeout_seconds=0)

    def test_fifo_order(self):
        q, _ = make_queue(visibility_timeout_seconds=10)
        q.enqueue("first")
        q.enqueue("second")
        self.assertEqual(q.dequeue().body, "first")
        self.assertEqual(q.dequeue().body, "second")

    def test_empty_queue_returns_none(self):
        q, _ = make_queue()
        self.assertIsNone(q.dequeue())


class VisibilityTimeoutTests(unittest.TestCase):
    def test_dequeued_message_is_invisible_until_timeout(self):
        q, clock = make_queue(visibility_timeout_seconds=30)
        q.enqueue("a")
        self.assertIsNotNone(q.dequeue())
        self.assertIsNone(q.dequeue())          # hidden during the timeout
        clock.advance(29)
        self.assertIsNone(q.dequeue())          # still hidden at 29s
        clock.advance(1)
        self.assertEqual(q.dequeue().body, "a")  # reappears at 30s

    def test_redelivery_counts_deliveries(self):
        q, clock = make_queue(visibility_timeout_seconds=10)
        q.enqueue("a")
        first = q.dequeue()
        self.assertEqual(first.deliveries, 1)
        clock.advance(10)
        again = q.dequeue()
        self.assertIs(again, first)
        self.assertEqual(again.deliveries, 2)

    def test_delete_acks_before_timeout(self):
        q, clock = make_queue(visibility_timeout_seconds=30)
        q.enqueue("a")
        msg = q.dequeue()
        self.assertTrue(q.delete(msg))
        clock.advance(60)
        self.assertIsNone(q.dequeue())          # deleted, not redelivered
        self.assertEqual(q.stats.deleted, 1)

    def test_delete_unknown_message_returns_false(self):
        q, _ = make_queue()
        other, _ = make_queue()
        msg = other.enqueue("x")
        self.assertFalse(q.delete(msg))


class DlqTests(unittest.TestCase):
    def test_moves_to_dlq_after_max_deliveries(self):
        q, clock = make_queue(visibility_timeout_seconds=10, max_deliveries=3)
        q.enqueue("poison")
        for _ in range(3):
            self.assertIsNotNone(q.dequeue())
            clock.advance(10)
        self.assertIsNone(q.dequeue())          # 4th attempt: dead-lettered
        self.assertEqual(q.dlq_count(), 1)
        self.assertEqual(q.pending_count(), 0)
        self.assertEqual(q.stats.dead_lettered, 1)

    def test_healthy_messages_still_flow_past_dlq_candidate(self):
        q, clock = make_queue(visibility_timeout_seconds=10, max_deliveries=2)
        q.enqueue("poison")
        q.enqueue("fine")
        self.assertEqual(q.dequeue().body, "poison")  # delivery 1 (FIFO)
        clock.advance(10)
        self.assertEqual(q.dequeue().body, "poison")  # delivery 2 = max_deliveries
        clock.advance(10)
        fine = q.dequeue()                            # poison dead-lettered, fine still flows
        self.assertEqual(fine.body, "fine")
        self.assertTrue(q.delete(fine))
        self.assertEqual(q.dlq_count(), 1)
        self.assertEqual(q.dlq[0].body, "poison")
        self.assertEqual(q.pending_count(), 0)


class ReplayTests(unittest.TestCase):
    def test_replay_resets_delivery_count(self):
        q, clock = make_queue(visibility_timeout_seconds=10, max_deliveries=1)
        q.enqueue("a")
        q.dequeue()
        clock.advance(10)
        q.dequeue()                             # to DLQ
        replayed = replay_dlq(q)
        self.assertEqual(len(replayed), 1)
        self.assertEqual(replayed[0].deliveries, 0)
        self.assertEqual(q.dequeue().body, "a")  # deliverable again

    def test_replay_empty_dlq_is_noop(self):
        q, _ = make_queue()
        self.assertEqual(replay_dlq(q), [])
        self.assertEqual(q.stats.replayed, 0)


if __name__ == "__main__":
    unittest.main()
