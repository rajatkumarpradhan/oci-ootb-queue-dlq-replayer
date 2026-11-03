"""Structural validation of the queue model against documented OCI behavior."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oci_queue_dlq_replayer import OciQueue, replay_dlq

NOW = [0.0]

def clock():
    return NOW[0]

checks = []

def check(name, ok):
    checks.append(ok)
    print(f"{'PASS' if ok else 'FAIL'} {name}")

# 1. visibility timeout hides a dequeued message
q = OciQueue("v", visibility_timeout_seconds=30, max_deliveries=5, clock=clock)
q.enqueue("m")
q.dequeue()
check("dequeued message invisible during visibility timeout", q.dequeue() is None)

# 2. message reappears after the timeout
NOW[0] += 30
check("message reappears after visibility timeout", q.dequeue() is not None)

# 3. delete acknowledges and prevents redelivery
q2 = OciQueue("d", visibility_timeout_seconds=10, clock=clock)
q2.enqueue("m")
q2.delete(q2.dequeue())
NOW[0] += 100
check("deleted message is never redelivered", q2.dequeue() is None)

# 4. max_deliveries routes to the DLQ
NOW[0] = 0.0
q3 = OciQueue("dlq", visibility_timeout_seconds=10, max_deliveries=2, clock=clock)
q3.enqueue("poison")
q3.dequeue(); NOW[0] += 10; q3.dequeue(); NOW[0] += 10
q3.dequeue()
check("message dead-letters after max_deliveries", q3.dlq_count() == 1)

# 5. DLQ replay restores deliverability with a fresh delivery budget
replay_dlq(q3)
msg = q3.dequeue()
check("replayed message deliverable with reset deliveries", msg is not None and msg.deliveries == 1)

print(f"validate: {sum(checks)}/{len(checks)} behavior checks passed")
sys.exit(0 if all(checks) else 1)
