"""End-to-end demo: a poison message flows to the DLQ, gets fixed, replays."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oci_queue_dlq_replayer import OciQueue, replay_dlq

NOW = [0.0]

def clock():
    return NOW[0]

def advance(seconds):
    NOW[0] += seconds


def main() -> int:
    queue = OciQueue("orders", visibility_timeout_seconds=30, max_deliveries=3, clock=clock)
    queue.enqueue("order-1001: ship 2 keyboards")
    queue.enqueue("order-1002: ship 1 mouse")
    queue.enqueue("POISON: unparseable payload")

    print("== producer enqueued 3 messages ==")

    def broken_handler(msg):
        if msg.body.startswith("POISON"):
            raise ValueError("cannot parse order")
        print(f"  processed: {msg.body}")
        return True

    print("== consumer with broken handler (max_deliveries=3, visibility=30s) ==")
    for attempt in range(1, 8):
        msg = queue.dequeue()
        if msg is None:
            print(f"  attempt {attempt}: nothing visible (invisibility window) - waiting 30s")
            advance(30)
            continue
        try:
            broken_handler(msg)
            queue.delete(msg)
        except ValueError as exc:
            print(f"  attempt {attempt}: {msg.message_id} failed delivery #{msg.deliveries}: {exc}")
            advance(30)  # wait out the visibility timeout so it reappears

    print(f"== after processing: pending={queue.pending_count()} dlq={queue.dlq_count()} ==")
    assert queue.dlq_count() == 1, "poison message should be in the DLQ"
    print(f"  DLQ holds: {queue.dlq[0].message_id} '{queue.dlq[0].body}' after {queue.dlq[0].deliveries} deliveries")

    print("== handler fixed (poison payload now routable) ==")
    def fixed_handler(msg):
        print(f"  processed: {msg.body}")
        return True

    replayed = replay_dlq(queue)
    print(f"== replayed {len(replayed)} message(s) from DLQ ==")
    while True:
        msg = queue.dequeue()
        if msg is None:
            break
        fixed_handler(msg)
        queue.delete(msg)

    s = queue.stats
    print(f"== final: enqueued={s.enqueued} delivered={s.delivered} deleted={s.deleted} "
          f"dead_lettered={s.dead_lettered} replayed={s.replayed} pending={queue.pending_count()} dlq={queue.dlq_count()} ==")
    assert queue.pending_count() == 0 and queue.dlq_count() == 0, "queue should be fully drained"
    assert s.deleted == 3 and s.dead_lettered == 1 and s.replayed == 1
    print("DEMO OK: poison message dead-lettered, replayed after fix, queue drained")
    return 0

if __name__ == "__main__":
    sys.exit(main())
