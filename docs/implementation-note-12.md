# Implementation Note 12: API Gateway Patterns — The Six Problems Every Production Gateway Must Solve

Every team that exposes a public API eventually builds the same six mechanisms from scratch, each time, slightly wrong. This note documents the problems, the tradeoffs, and the reference implementations in `14_api_gateway_patterns.py`.

---

## The Problem With "Just Use a Library"

Off-the-shelf API gateways (Kong, AWS API Gateway, Envoy) solve these problems for HTTP proxies. But application-layer gateways — the ones you write in Python to protect your own services — need these mechanisms implemented in code. A rate limiter in Nginx does not protect a downstream gRPC service. A circuit breaker in your service mesh does not prevent a Python worker from hammering a failing third-party API.

The six mechanisms below are needed in application code, not just at the network edge.

---

## Problem 1: Rate Limiting — Two Algorithms, Different Tradeoffs

Rate limiting sounds simple until you hit the boundary between two time windows.

### The Token Bucket (Burst-Friendly)

The token bucket refills at a constant rate and allows burst traffic up to `bucket_capacity`. A client with a 10 req/s limit and a capacity of 50 can send 50 requests instantly after being idle, then is limited to 10/s going forward. This is the right algorithm when:

- Clients have bursty, legitimate traffic patterns (mobile apps, batch jobs)
- You want to smooth backend load without rejecting valid bursts
- The burst size is bounded and known

The tradeoff: a sustained burst uses the entire bucket. A client that idles for 60 seconds with a 10/s rate gets 50 tokens (at capacity=50), not 600. Idle time does not accumulate beyond capacity.

### The Sliding Window Log (Exact Counting)

The sliding window log records the timestamp of every request. On each request, timestamps older than `window_ms` are pruned; if the count equals the limit, the request is rejected. This provides:

- Exact enforcement: no request is ever counted in two windows
- No burst: the limit is enforced over any `window_ms` period, not just calendar windows
- Higher memory: O(limit) timestamps per client

The tradeoff: memory grows with the limit. For a 10,000 req/min limit per client, you store up to 10,000 timestamps. The fixed-window algorithm is O(1) but has a known 2× burst problem at window boundaries (requests pile up at the end of window N and start of window N+1).

**Rule of thumb:** Use token bucket for user-facing APIs with legitimate bursts. Use sliding window for security-critical or billing-critical limits where the 2× burst artifact is unacceptable.

---

## Problem 2: Circuit Breaker — Stop Amplifying Failures

When a backend is slow or failing, clients retry. Retries make the backend slower. The backend fails harder. This is cascading failure, and it is the most common cause of site-wide outages.

The circuit breaker stops it by refusing to forward requests to a failing backend for a timeout period, then probing with a single request to check for recovery.

### State Machine

```
CLOSED → (failure_threshold consecutive failures) → OPEN
OPEN   → (timeout_ms elapsed)                    → HALF_OPEN (probe)
HALF_OPEN → (success)                             → CLOSED
HALF_OPEN → (failure)                             → OPEN (doubled timeout)
```

### Exponential Backoff on Re-Trip

When a probe in HALF_OPEN fails, the next timeout is doubled (up to `max_timeout_ms`). Without this, a backend that recovers for 100ms then degrades again will get probed aggressively — each probe keeps the window short, and traffic floods in the moment recovery is detected. Doubling the timeout gives the backend time to stabilize.

### The success_threshold Parameter

Returning to CLOSED on a single success can be dangerous. If the backend is flapping, the first success closes the circuit and full traffic resumes immediately. `success_threshold=2` (or more) requires consecutive successes before declaring recovery. The right value depends on your backend's stability characteristics.

### What Circuit Breakers Do Not Do

A circuit breaker does not retry requests. It rejects them fast. The client receives an error immediately and can decide whether to use a fallback, cache, or queue. The circuit breaker's job is to stop sending load to a failing backend — not to hide the failure from the client.

---

## Problem 3: Request Coalescing — Zero Redundant Backend Calls

When a popular resource expires from cache, every concurrent request for it hits the backend simultaneously. This is the thundering herd / cache stampede problem. The distributed lock approach (see Implementation Note 11) prevents stampede by having only one process fetch at a time. But the other processes still wait and then fetch again.

Request coalescing is different: only the first request is forwarded to the backend. All others wait and receive the exact same result as the first request. The backend is called exactly once, regardless of how many concurrent requests arrive.

### Implementation

```python
if key in self._in_flight:
    # Register as waiter — wait for the first request to complete
    event = threading.Event()
    self._waiters[key].append((event, result_box))
else:
    # First request — mark as in-flight and fetch
    self._in_flight[key] = True
```

The `threading.Event` is set by the first request when its fetch completes. All waiters receive the same result from `result_box[0]`.

### Coalescing vs. Caching

These are complementary, not alternatives:

| Mechanism | Deduplicates | Duration | Use Case |
|-----------|-------------|----------|----------|
| Coalescing | Concurrent in-flight requests only | Until first response | Stampede prevention |
| Cache | All requests for TTL | TTL duration | General read acceleration |

A system should typically have both: coalescing prevents stampede when the cache miss is concurrent, and caching prevents the miss from happening repeatedly.

---

## Problem 4: API Key Manager — Keys Are Not Passwords

API keys have a lifecycle that passwords do not: they expire, they rotate with grace periods, and they carry tier-based permissions.

### Key Points

**Never store raw keys.** The stored `key_hash` is SHA-256 of the raw key. The raw key is only returned at creation time. If your database is breached, the attacker gets hashes, not usable keys.

**Rotation with grace period.** When a key is rotated, the old key remains valid for `grace_period_ms` (default: 24 hours). This allows clients to deploy new key configuration without a hard cutover. Both keys validate successfully during the grace period.

**Tier-based rate limits.** Rate limits are attached to the key, not to a separate rate-limiter lookup. The `rate_limit_rpm` field is set at key creation from the tier table. This means rate limit changes require key rotation — which is intentional; it creates an audit trail.

### Key Lifecycle

```
ACTIVE → (expires_at - now < rotation_warning_days) → EXPIRING_SOON
ACTIVE or EXPIRING_SOON → (now >= expires_at) → EXPIRED
ACTIVE → (revoke_key called) → REVOKED
```

EXPIRING_SOON keys still validate successfully. The status change is informational — it signals to the client in the validation response that rotation is needed, not that the key is rejected.

---

## Problem 5: API Version Router — Sunset Before You Deprecate

Teams typically deprecate API versions after they stop supporting them. This is backwards. The right order is:

1. Release new version
2. Announce deprecation of old version with a specific sunset date (Sunset header, RFC 8594)
3. At sunset: return 410 Gone

The Sunset header tells clients exactly when the version will be retired. Without it, clients don't know when they must migrate by — and "eventually" means "never."

### Version Resolution Order

```
1. API-Version header (most explicit)
2. URL prefix (/v2/resource → "v2")
3. Default: current version
```

Unknown versions fall back to current rather than returning an error. This is debatable — failing fast on unknown versions makes client bugs visible immediately. The fallback approach is more tolerant but masks version header typos. Choose based on how strictly you want to enforce version discipline.

---

## Problem 6: Idempotency Key — Structure Over Discipline

"Just don't retry if the first request succeeded" is not a production strategy. Networks fail. Load balancers time out. Mobile clients go offline mid-request. The client cannot know if the first request succeeded — it only knows it did not receive a response.

The idempotency key pattern: the client generates a UUID, sends it with the request, and the server stores the response. If the same key arrives again within the TTL, the stored response is returned without re-executing the handler.

### What Idempotency Prevents

- Double-charges: payment POST with same idempotency key → second request returns first response, charge not duplicated
- Duplicate resource creation: POST /orders with retried key → second request returns first order, second order not created
- Double-submission: form submission with same key → duplicate form submission blocked

### TTL Design

The TTL should be at least as long as the client's retry window plus the client's deployment window. A 24-hour TTL handles:
- Network retries within seconds
- Client-side retry logic within minutes
- Mobile apps that go offline and retry on reconnect (up to hours)

A 30-minute TTL is insufficient for mobile clients. A 7-day TTL is safe for most applications but increases storage requirements.

### Idempotency Is Not Exactly-Once Delivery

Idempotency prevents duplicate side effects when the client retries. It does not guarantee that the handler is called exactly once — in a distributed system, the handler may be called once on the first server and once on a second server before the idempotency store is written. For true exactly-once semantics, combine idempotency keys with transactional outbox patterns or distributed consensus.

---

## Summary: Which Mechanism Solves Which Problem

| Problem | Mechanism | Key Parameter |
|---------|-----------|---------------|
| Abuse / overload | Token Bucket | `bucket_capacity` for burst tolerance |
| Boundary burst artifacts | Sliding Window Log | exact but memory-heavy |
| Cascading failure | Circuit Breaker | `failure_threshold`, `timeout_ms` |
| Cache stampede (concurrent) | Request Coalescer | `key_fn` for request identity |
| Key lifecycle / tier permissions | API Key Manager | `tier`, `grace_period_ms` |
| Version lifecycle enforcement | Version Router | `sunset_at`, Sunset header |
| Duplicate side effects from retries | Request Deduplicator | `ttl_ms`, client-provided key |

None of these mechanisms are optional in a production API. The question is whether you implement them deliberately, with clear tradeoffs documented, or discover them one production incident at a time.
