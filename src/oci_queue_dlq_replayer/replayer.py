"""DLQ replay: move dead-lettered messages back for another attempt."""
from __future__ import annotations

from .queue import OciQueue


def replay_dlq(queue: OciQueue) -> list:
    """Move every DLQ message back to the main queue for reprocessing.

    Delivery counters reset, so a message whose handler is now fixed gets a
    fresh max_deliveries budget - matching an OCI Queue DLQ replay.
    Returns the replayed messages.
    """
    replayed = []
    while queue.dlq:
        msg = queue.dlq.pop(0)
        msg.deliveries = 0
        msg.visible_at = 0.0
        msg.receipt = ""
        queue._messages.append(msg)
        queue.stats.replayed += 1
        replayed.append(msg)
    return replayed
