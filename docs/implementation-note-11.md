# Implementation Note 11 — Distributed Cache Patterns: When Adding a Cache Makes Things Harder

Example scope: snippets below include application wiring and internal excerpts from
[`examples/13_distributed_cache_patterns.py`](../examples/13_distributed_cache_patterns.py); undefined names are supplied by that example or the application.
They are not standalone package APIs. See the linked module for complete setup.


## Overview

Despite its name, `DistributedLockManager` uses an in-process dictionary and thread
lock. `CacheInvalidationBus` also runs in process. They demonstrate local behavior;
sharing a lock or invalidation across nodes requires an external implementation.

Every engineer knows that caching is one of the two hard problems in computer science.
The other is cache invalidation — and that's only the start of the list. This note
covers the categories of distributed cache failures that `13_distributed_cache_patterns.py`
addresses, and explains the design decisions behind each pattern.

---

## The Cache Failures Covered Here

### 1. Cache Stampede (Thundering Herd)

**What goes wrong:** A popular cache entry expires at time T. Before it's regenerated,
100 concurrent requests all miss the cache and all hit the backing database simultaneously.
The database, which your cache was supposed to protect, now receives a spike exactly as
large as the worst-case concurrent load.

**What the pattern does:** `CacheAsidePattern` acquires a distributed lock (`DistributedLockManager`)
before regenerating the entry. The first request gets the lock, loads from the store,
and populates the cache. All other concurrent requests either:
- Poll the cache briefly (lock-wait path)
- Return `None` (fail-open path, appropriate for non-critical data)

**TTL jitter prevents synchronized expiry:** `TTLInvalidationStrategy.compute_ttl_ms()`
adds random jitter (default ±20%) to every TTL. If 1,000 keys are populated at the
same time, they expire over a 40% window rather than all at once. This is the cheapest
stampede prevention available — a way to spread expiry times; it does not eliminate stampedes for an individual key.

```python
# Without jitter: 1000 keys all expire at T+30s
cache.set(key, value, ttl_ms=30_000)

# With jitter: keys expire between T+24s and T+36s
ttl = strategy.compute_ttl_ms(base_override=30_000)  # 24000–36000
cache.set(key, value, ttl_ms=ttl)
```

---

### 2. Write Consistency: Choosing Your Tradeoff

There is no "correct" write strategy — there are only tradeoffs that match or don't
match your consistency requirements:

| Strategy | Write Latency | Read Freshness | Durability Risk |
|----------|--------------|----------------|-----------------|
| Write-Through | cache + store | Depends on write completion and invalidation | Partial-write failure |
| Write-Behind | cache only (fast) | Fresh (cache) | Loss on crash |
| Cache-Aside | store only (write path uncached) | Stale until invalidation/TTL | Depends on backing store |

**Write-Through** (`WriteThroughCache`): Every `put()` synchronously updates both
cache and store. The two writes are not atomic; failures and concurrent writers still need recovery. Appropriate for transactional data
(orders, payments) where stale reads are unacceptable and write latency is tolerable.

**Write-Behind** (`WriteBehindCache`): Writes update the cache immediately and are
buffered in a `dict` (in this reference; a durable queue in production). The store
is updated asynchronously via `flush()`. Buffering avoids a backing-store write on each call; the example does not establish
a production throughput or latency improvement.

**The durability risk of write-behind is real.** If the cache node fails between a
write and the next flush, the buffered data is lost. Production implementations
mitigate this by using durable write-behind queues:
- **Kafka**: write intent to a topic; a consumer applies to the database
- **Redis Streams**: XADD to a stream; a consumer group applies to the database
- **Outbox pattern**: write to a local DB table; a CDC connector reads and applies

The reference implementation uses an in-process `dict` for demonstration. Do not
use it as-is for durability-sensitive workloads.

---

### 3. Cache Invalidation Across Nodes

**The hard problem:** You have three cache nodes (replicas, or a cluster). A user
updates their profile on node A. Nodes B and C still hold the old profile. Under
TTL expiry, they'll serve stale data for up to the full TTL window.

**What the pattern does:** `CacheInvalidationBus` is a publish/subscribe channel.
When a node invalidates a key, it publishes an `InvalidationEvent`. All other nodes
subscribe and immediately evict the key from their local store.

```
Node A: user:1001 updated → delete locally → publish("user:1001", EXPLICIT_DELETE)
Node B: receives event → delete locally (eviction_count += 1)
Node C: receives event → delete locally (eviction_count += 1)
```

`InvalidatingCache.invalidate_and_publish()` is the single method that combines
local eviction and bus publication. This sequences the two operations in one method; it does not make eviction and
publication atomic or recover a failed publication.

**Source node filtering:** A node does not apply events it published itself
(`if event.source_node_id == self.node_id: return`). This prevents a node from
receiving its own publication and unnecessarily clearing a key it just set.

**Production transports:**
- **Redis Pub/Sub**: simple, at-most-once delivery; suitable for non-critical invalidations
- **Kafka**: durable transport with configuration-dependent delivery semantics; consumers still need replay handling
- **Redis Streams + consumer groups**: durable, supports consumer acknowledgment

---

### 4. Dependency Invalidation

Simple TTL and explicit-key invalidation work for direct relationships. When a parent
record changes, all derived keys must also be invalidated — but the dependencies may
not be obvious at the call site.

`TTLInvalidationStrategy.register_dependency()` builds an explicit dependency graph.
`get_dependent_keys()` walks the graph transitively to return the complete set of
keys that must be evicted:

```python
strategy.register_dependency("user:1001", "user:1001:orders")
strategy.register_dependency("user:1001", "user:1001:cart")
strategy.register_dependency("user:1001:orders", "user:1001:orders:summary")

# When user:1001 changes:
keys = strategy.get_dependent_keys("user:1001")
# → {"user:1001:orders", "user:1001:cart", "user:1001:orders:summary"}
```

The implementation's visited set prevents infinite traversal through cycles.
Registration does not reject cycles; validate the intended dependency model separately.

---

### 5. Fencing Tokens and Split-Brain Writes

The distributed lock prevents simultaneous cache regeneration, but it doesn't prevent
a subtler failure: **a lock holder paused after acquiring the lock**.

**Scenario:**
1. Worker A acquires lock, starts regenerating entry
2. Worker A is paused (GC pause, network partition) for > TTL
3. Lock expires; Worker B acquires lock, regenerates entry, commits to database
4. Worker A resumes and commits its (now stale) value to the database — overwriting B's fresh value

**What fencing tokens prevent:** Every lock grant increments a monotonic sequence number.
The lock holder must pass its token sequence when writing to the backing store. The store
(Redis, RDBMS, or custom) rejects writes with a sequence number lower than the highest
seen sequence for that lock key.

```python
token = lock_mgr.acquire("record:500", holder_id="w1", ttl_ms=0)  # immediately expired (demo)
token2 = lock_mgr.acquire("record:500", holder_id="w2")  # sequence=2

# Worker A resumes and tries to write with stale token
assert lock_mgr.validate_token(token) is False  # rejected — sequence 1 < 2
```

In the reference implementation, `validate_token()` is a check the caller must make
before writing. In production, this check belongs in the backing store itself
(RDBMS `WHERE version = expected_version`, Redis `WATCH`/`MULTI`/`EXEC`).

---

### 6. Cache Warmup: Preventing the Cold Start Spike

**What goes wrong:** You deploy a new instance (or restart after eviction). The cache
is empty. The first N requests all miss and hit the database simultaneously — exactly
the thundering herd, but at deployment rather than TTL expiry.

**Three warmup strategies in `CacheWarmupStrategy`:**

1. **Snapshot warmup**: Load a pre-built `WarmupRecord` list with priority ordering.
   High-priority keys (product catalog, configuration, active sessions) load first,
   allowing the application to begin serving traffic with partial warmup before low-priority
   keys are loaded.

2. **Key list replay**: Given a list of keys (from access logs), load values from the
   backing store. Accurately reflects real traffic patterns since it replays what was
   actually accessed.

3. **Prefix-based warmup**: Load all keys matching a prefix from the store. Useful for
   deterministic data sets (all configuration keys, all enum tables, all tenant metadata).

**`max_warmup_keys` provides flow control.** Without a limit, warmup on a large dataset
could cause a write storm to the cache (filling memory), a read storm to the backing
store (loading all keys), or both. The limit allows you to tradeoff warmup completeness
against startup time.

---

## Decision Guide: Which Pattern for Which Use Case?

| Use Case | Recommended Pattern(s) |
|----------|------------------------|
| Read-heavy, occasional writes | Cache-Aside + TTL jitter |
| Write-heavy, read consistency required | Write-Through |
| Write-heavy, throughput > durability | Write-Behind + durable queue |
| Multi-node cluster | Invalidation Bus (Redis Pub/Sub or Kafka) |
| Parent/child key relationships | TTL Dependency Invalidation |
| Concurrent cache regeneration | Cache-Aside + Distributed Lock |
| Post-deployment cold start | Cache Warmup Strategy |

---

## References

- Kleppmann, M. *Designing Data-Intensive Applications* (2017) — Ch. 8 (distributed locks, fencing tokens)
- Redis documentation — Redlock algorithm, Pub/Sub, Streams
- Sam Newman, *Building Microservices*, 2nd ed. (2021) — distributed service design
- AWS ElastiCache best practices — thundering herd prevention, write-behind with SQS
- Facebook engineering blog — Memcached at scale (lease-based stampede prevention)
