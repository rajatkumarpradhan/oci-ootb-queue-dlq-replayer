"""In-memory model of OCI Queue semantics.

Models the documented delivery behavior of Oracle Cloud Infrastructure Queue:
a dequeued message stays invisible for the visibility timeout; if it is not
deleted (acknowledged) before the timeout, it reappears for redelivery; after
max_deliveries attempts it moves to the dead letter queue (DLQ).

This is a local model for learning and testing - it makes no network calls.
"""
from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field


@dataclass
class Message:
    """One message. body is the payload; deliveries counts dequeue attempts."""
    body: str
    message_id: str = ""
    deliveries: int = 0
    visible_at: float = 0.0
    receipt: str = field(default="")


@dataclass
class QueueStats:
    enqueued: int = 0
    delivered: int = 0
    deleted: int = 0
    dead_lettered: int = 0
    replayed: int = 0


class OciQueue:
    """A queue with visibility timeout and DLQ semantics.

    Parameters mirror OCI Queue configuration:
    - visibility_timeout_seconds: how long a dequeued message stays hidden.
    - max_deliveries: deliveries after which a message moves to the DLQ.
    """

    def __init__(self, name: str, visibility_timeout_seconds: float = 30.0,
                 max_deliveries: int = 5, clock=time.monotonic):
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be >= 1")
        if visibility_timeout_seconds <= 0:
            raise ValueError("visibility timeout must be positive")
        self.name = name
        self.visibility_timeout = float(visibility_timeout_seconds)
        self.max_deliveries = int(max_deliveries)
        self._clock = clock
        self._ids = itertools.count(1)
        self._receipts = itertools.count(1)
        self._messages: list[Message] = []
        self.dlq: list[Message] = []
        self.stats = QueueStats()

    def enqueue(self, body: str) -> Message:
        if not isinstance(body, str) or not body:
            raise ValueError("message body must be a non-empty string")
        msg = Message(body=body, message_id=f"{self.name}-m{next(self._ids)}")
        self._messages.append(msg)
        self.stats.enqueued += 1
        return msg

    def _visible(self, now: float) -> Message | None:
        for msg in self._messages:
            if msg.visible_at <= now:
                return msg
        return None

    def dequeue(self) -> Message | None:
        """Return the next visible message and hide it for the timeout.

        A message that has been delivered max_deliveries times moves to the
        DLQ instead of being delivered again.
        """
        now = self._clock()
        while True:
            msg = self._visible(now)
            if msg is None:
                return None
            if msg.deliveries >= self.max_deliveries:
                self._messages.remove(msg)
                self.dlq.append(msg)
                self.stats.dead_lettered += 1
                continue
            msg.deliveries += 1
            msg.visible_at = now + self.visibility_timeout
            msg.receipt = f"rcpt-{next(self._receipts)}"
            self.stats.delivered += 1
            return msg

    def delete(self, msg: Message) -> bool:
        """Acknowledge successful processing. Returns False on unknown message."""
        if msg in self._messages:
            self._messages.remove(msg)
            self.stats.deleted += 1
            return True
        return False

    def pending_count(self) -> int:
        return len(self._messages)

    def dlq_count(self) -> int:
        return len(self.dlq)
