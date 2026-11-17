# Backend Systems Lab

Two runnable, AI-assisted practice projects in backend correctness. These are learning exercises, not production systems or employer work. Understand and modify the implementation before presenting it in an interview.

## SlotGuard (Java 17+)

Run `java SlotGuard.java`. A single service lock makes slot reservation and idempotency records atomic. The executable checks retries, mismatched payloads, input validation and 100 concurrent requests competing for one slot. Exactly one wins. State is in memory and lost on restart. The lock serializes all requests and does not coordinate multiple instances. A database-backed version needs unique slot constraints and persistent idempotency in one transaction. No HTTP API or authentication is implemented.

## Durable outbox (Python 3.10+)

Run `python -m unittest -v`. Standard-library SQLite atomically creates a campaign and its background job. Duplicate request keys reuse the existing job; mismatched payloads are rejected. The worker uses exponential retry delays and a dead-letter state. Nine tests cover concurrent enqueue, retries, persistence, delivery, backoff, empty queues, input validation and rollback. No messages are sent externally.

Delivery is at least once: a crash after an external side effect but before commit can repeat it. Downstream consumers must deduplicate by the stable delivery key. The single-worker example holds a write transaction during delivery, so slow delivery blocks writers. Production improvements include leases, timeouts, authentication, monitoring and dead-letter recovery. No exactly-once claim is made.

## Interview questions

- Why is a read-before-write check insufficient without atomicity?
- How do in-process locks differ from database constraints?
- What happens between an external effect and commit?
- Why validate payloads when reusing idempotency keys?
- How would these cores become authenticated APIs?
