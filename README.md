# oci-ootb-queue-dlq-replayer

A runnable model of **OCI Queue** out-of-the-box delivery semantics - visibility
timeout, redelivery, dead letter queue (DLQ) and DLQ replay - with a poison-message
demo that shows the full lifecycle end to end.

## The OCI feature, out of the box

[OCI Queue](https://www.oracle.com/cloud/queue/) is a fully managed, serverless queue.
Oracle's published pricing makes the first **1,000,000 requests per month free**
($0.22 per million requests after that), so this pattern costs nothing to run for
real at demo scale.
Source: <https://www.oracle.com/cloud/queue/pricing/>

The delivery contract this repo models:

1. `dequeue` returns a message and hides it for the **visibility timeout**.
2. If the consumer never calls `delete` (ack) before the timeout, the message
   **reappears** and is redelivered.
3. After **max_deliveries** attempts, the message moves to the **DLQ** instead of
   looping forever.
4. A **DLQ replay** returns dead-lettered messages to the main queue with a fresh
   delivery budget - the flow for "fix the handler, then replay".

## Try it

```bash
pip install -e .
python3 examples/poison_message_demo.py   # full poison-message lifecycle
python3 -m unittest discover -s tests     # 13 unit tests
python3 scripts/validate.py               # 5 behavior checks vs the contract above
```

The demo enqueues three orders where one payload is poison. The consumer fails it
three times, OCI Queue semantics park it in the DLQ, the handler is fixed, a replay
moves it back, and the queue drains. Final counters are asserted, not just printed:

```
== final: enqueued=3 delivered=6 deleted=3 dead_lettered=1 replayed=1 pending=0 dlq=0 ==
DEMO OK: poison message dead-lettered, replayed after fix, queue drained
```

## Why this matters

Every production queue eventually meets a poison message. Without a DLQ, one bad
payload blocks everything behind it; without replay, recovery means manual surgery.
Knowing exactly when a message dead-letters (deliveries vs max_deliveries, not
retries vs timeout) is the difference between a queue that heals and a pager.

## Honest limits

- **Local model, no broker**: pure Python, in-memory, deterministic fake clock. No
  OCI SDK, no network calls, no tenancy required. It models documented behavior; it
  is not OCI Queue itself.
- Single-consumer, in-order visibility is modeled; real queues add at-least-once
  races between concurrent consumers.
- To run this against real OCI Queue: create a queue in the Console (free tier
  covers it), set max deliveries on the DLQ policy, and use `oci.queue` SDK calls -
  the semantics here map 1:1 to `PutMessages` / `GetMessages` / `DeleteMessages`.

Built by Rajat Kumar Pradhan as part of a daily OCI portfolio practice.
