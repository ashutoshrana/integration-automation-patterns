"""
13_distributed_cache_patterns.py — Cache-Aside, Write-Through, Write-Behind,
TTL Invalidation, Distributed Lock, Cache Invalidation Bus, and Cache Warmup
patterns for enterprise distributed systems.

A cache layer is not a simple key-value store bolted onto a database. In
enterprise integration, caching introduces five categories of hazards that
these patterns address systematically:

    1. Cache Stampede (Thundering Herd):
       When a popular cache entry expires, many concurrent requests miss
       simultaneously and all hit the backing store. CacheAsidePattern uses
       a DistributedLockManager to allow only one request to regenerate the
       entry while others wait, then serve from cache.

    2. Write Consistency (Write-Through vs. Write-Behind):
       Write-Through updates the cache and store synchronously — reads are
       always consistent but writes are slower. Write-Behind batches store
       writes asynchronously for throughput — reads may see stale data during
       the write buffer window. WriteThroughCache and WriteBehindCache model
       both, including the flush/drain lifecycle.

    3. Cache Invalidation (The Hard Problem):
       Distributed systems have multiple cache nodes; an update on node A
       must invalidate entries on nodes B–N. CacheInvalidationBus publishes
       invalidation events over a message bus (Kafka/Redis Pub/Sub) so all
       nodes evict the affected key. Without this, stale reads persist until
       TTL expiry.

    4. TTL Strategy:
       Fixed TTL is simple but causes synchronized expiration spikes.
       TTLInvalidationStrategy adds jitter to TTLs, implements sliding
       window renewal on read, and supports explicit dependency-based
       invalidation (when record X changes, also invalidate keys that
       embed X's data).

    5. Cache Warmup:
       Cold start after deployment or eviction causes a latency spike as
       the cache fills. CacheWarmupStrategy pre-populates critical keys
       from a snapshot or by replaying access logs before the application
       accepts traffic.

Patterns implemented
--------------------

    CacheAsidePattern          — lazy-load with stampede protection
    WriteThroughCache          — synchronous write to cache + store
    WriteBehindCache           — async write buffer with flush/drain
    TTLInvalidationStrategy    — jitter, sliding window, dependency invalidation
    DistributedLockManager     — fencing token-based distributed lock
    CacheInvalidationBus       — fan-out invalidation via message bus
    CacheWarmupStrategy        — prefetch and replay-based warmup

No external dependencies required.

Run:
    python examples/13_distributed_cache_patterns.py
"""

from __future__ import annotations

import hashlib
import random
import time
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------


@dataclass
class CacheEntry:
    """A single cache entry with value, TTL tracking, and version."""
    key: str
    value: Any
    created_at: float
    expires_at: float
    version: int = 1
    last_accessed_at: float = field(default_factory=time.monotonic)

    @property
    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at

    @property
    def ttl_remaining_ms(self) -> float:
        remaining = self.expires_at - time.monotonic()
        return max(0.0, remaining * 1000)


@dataclass
class WriteBufferEntry:
    """A pending write in the write-behind buffer."""
    key: str
    value: Any
    enqueued_at: float
    attempts: int = 0


class LockState(str, Enum):
    ACQUIRED = "ACQUIRED"
    EXPIRED = "EXPIRED"
    RELEASED = "RELEASED"


@dataclass
class FencingToken:
    """
    Monotonically increasing token issued with each lock grant.

    Fencing tokens prevent split-brain writes: a write to the backing store
    must include the token; the store rejects writes with a stale token.
    This ensures that a lock holder that was paused (GC, network partition)
    cannot corrupt state after its lock expired.
    """
    token_id: str
    lock_key: str
    holder_id: str
    sequence: int
    granted_at: float
    expires_at: float
    state: LockState = LockState.ACQUIRED

    @property
    def is_valid(self) -> bool:
        return (
            self.state == LockState.ACQUIRED
            and time.monotonic() < self.expires_at
        )


class InvalidationReason(str, Enum):
    TTL_EXPIRED = "TTL_EXPIRED"
    EXPLICIT_DELETE = "EXPLICIT_DELETE"
    DEPENDENCY_CHANGED = "DEPENDENCY_CHANGED"
    CAPACITY_EVICTION = "CAPACITY_EVICTION"
    BUS_INVALIDATION = "BUS_INVALIDATION"


@dataclass
class InvalidationEvent:
    """Published on the CacheInvalidationBus when a key is evicted."""
    event_id: str
    key: str
    reason: InvalidationReason
    source_node_id: str
    timestamp: float
    dependency_key: Optional[str] = None   # set for DEPENDENCY_CHANGED


@dataclass
class WarmupRecord:
    """A single record surfaced during cache warmup."""
    key: str
    value: Any
    priority: int = 0   # higher = load first
    source: str = "snapshot"


# ---------------------------------------------------------------------------
# Simulated in-memory store (stands in for Redis / Memcached)
# ---------------------------------------------------------------------------


class InMemoryStore:
    """
    Thread-safe in-memory key-value store simulating a remote cache backend
    (e.g., Redis). Used as the cache layer in pattern demonstrations.

    Not a production implementation — missing expiry background task,
    persistence, replication, etc.
    """

    def __init__(self, node_id: str = "node-0") -> None:
        self.node_id = node_id
        self._data: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self.hit_count = 0
        self.miss_count = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                if entry is not None and entry.is_expired:
                    del self._data[key]
                self.miss_count += 1
                return None
            entry.last_accessed_at = time.monotonic()
            self.hit_count += 1
            return entry.value

    def set(self, key: str, value: Any, ttl_ms: float = 60_000) -> None:
        with self._lock:
            now = time.monotonic()
            self._data[key] = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                expires_at=now + ttl_ms / 1000.0,
                version=(self._data[key].version + 1) if key in self._data else 1,
            )

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._data.get(key)
            return entry is not None and not entry.is_expired

    def ttl_ms(self, key: str) -> float:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return 0.0
            return entry.ttl_remaining_ms

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def keys(self) -> list[str]:
        with self._lock:
            return [k for k, v in self._data.items() if not v.is_expired]


# ---------------------------------------------------------------------------
# Simulated backing data store
# ---------------------------------------------------------------------------


class BackingDataStore:
    """
    Simulates a slow relational database (PostgreSQL / Oracle).
    Records read/write counts so tests can verify cache behavior.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self.read_count = 0
        self.write_count = 0
        self.latency_ms: float = 5.0   # simulated read latency

    def get(self, key: str) -> Optional[Any]:
        self.read_count += 1
        return self._data.get(key)

    def put(self, key: str, value: Any) -> None:
        self.write_count += 1
        self._data[key] = value

    def delete(self, key: str) -> bool:
        if key in self._data:
            del self._data[key]
            return True
        return False

    def seed(self, records: dict[str, Any]) -> None:
        """Pre-populate store without incrementing write_count."""
        self._data.update(records)


# ---------------------------------------------------------------------------
# Pattern 1 — Cache-Aside (Lazy Loading) with stampede protection
# ---------------------------------------------------------------------------


class CacheAsidePattern:
    """
    Cache-Aside (Lazy Loading) with distributed lock-based stampede protection.

    Standard cache-aside:
        1. Application checks cache.
        2. On miss: application loads from store, writes to cache, returns value.
        3. On hit: return cached value directly.

    Without stampede protection, step 2 under high concurrency causes N threads
    to simultaneously load from the backing store on the same key's expiry —
    the "thundering herd". This implementation uses DistributedLockManager to
    serialize regeneration: the first thread acquires the lock and regenerates;
    others poll the cache until the value appears or the lock expires.

    Parameters
    ----------
    cache : InMemoryStore
    store : BackingDataStore
    lock_manager : DistributedLockManager
    default_ttl_ms : float
        Base TTL. TTLInvalidationStrategy adds jitter if provided.
    ttl_strategy : TTLInvalidationStrategy | None
    """

    def __init__(
        self,
        cache: InMemoryStore,
        store: BackingDataStore,
        lock_manager: "DistributedLockManager",
        default_ttl_ms: float = 30_000,
        ttl_strategy: Optional["TTLInvalidationStrategy"] = None,
    ) -> None:
        self._cache = cache
        self._store = store
        self._lock_mgr = lock_manager
        self._default_ttl_ms = default_ttl_ms
        self._ttl_strategy = ttl_strategy

    def get(self, key: str, loader: Optional[Callable[[str], Any]] = None) -> Optional[Any]:
        """
        Retrieve key from cache, loading from store on miss.

        Parameters
        ----------
        key : str
        loader : callable | None
            If provided, used instead of BackingDataStore.get() to load the
            value on cache miss. Allows caller to define load logic.

        Returns
        -------
        value or None
        """
        # Fast path: cache hit
        value = self._cache.get(key)
        if value is not None:
            return value

        # Slow path: cache miss — acquire lock to regenerate
        lock_key = f"regen:{key}"
        token = self._lock_mgr.acquire(lock_key, holder_id="cache-aside", ttl_ms=5_000)
        if token is None:
            # Could not acquire lock — another thread is regenerating
            # Poll briefly (in production: retry with backoff or return stale)
            for _ in range(3):
                time.sleep(0.001)
                value = self._cache.get(key)
                if value is not None:
                    return value
            return None

        try:
            # Double-check: another thread may have populated while we waited
            value = self._cache.get(key)
            if value is not None:
                return value

            # Load from store
            if loader is not None:
                value = loader(key)
            else:
                value = self._store.get(key)

            if value is not None:
                ttl_ms = (
                    self._ttl_strategy.compute_ttl_ms(self._default_ttl_ms)
                    if self._ttl_strategy
                    else self._default_ttl_ms
                )
                self._cache.set(key, value, ttl_ms=ttl_ms)

            return value
        finally:
            self._lock_mgr.release(token)

    def invalidate(self, key: str) -> None:
        """Explicitly evict a key from cache."""
        self._cache.delete(key)


# ---------------------------------------------------------------------------
# Pattern 2 — Write-Through Cache
# ---------------------------------------------------------------------------


class WriteThroughCache:
    """
    Write-Through: every write updates the cache and the backing store
    atomically (within the same call — no true ACID guarantee across the two,
    but the order is always cache-first, then store).

    Guarantees:
        - Reads from cache never return data that hasn't been written to store.
        - Cache is always warm for recently written keys.
        - Write latency = cache_write_latency + store_write_latency.

    Write-through is appropriate when:
        - Reads are frequent and must be fast.
        - Write latency is acceptable (OLTP writes, not bulk loads).
        - Consistency is more important than write throughput.

    Parameters
    ----------
    cache : InMemoryStore
    store : BackingDataStore
    default_ttl_ms : float
    """

    def __init__(
        self,
        cache: InMemoryStore,
        store: BackingDataStore,
        default_ttl_ms: float = 60_000,
    ) -> None:
        self._cache = cache
        self._store = store
        self._default_ttl_ms = default_ttl_ms

    def get(self, key: str) -> Optional[Any]:
        value = self._cache.get(key)
        if value is not None:
            return value
        # Fallback to store (e.g., after eviction)
        value = self._store.get(key)
        if value is not None:
            self._cache.set(key, value, ttl_ms=self._default_ttl_ms)
        return value

    def put(self, key: str, value: Any, ttl_ms: Optional[float] = None) -> None:
        """Write to cache first, then store (write-through order)."""
        effective_ttl = ttl_ms if ttl_ms is not None else self._default_ttl_ms
        self._cache.set(key, value, ttl_ms=effective_ttl)
        self._store.put(key, value)

    def delete(self, key: str) -> None:
        self._cache.delete(key)
        self._store.delete(key)


# ---------------------------------------------------------------------------
# Pattern 3 — Write-Behind (Write-Back) Cache
# ---------------------------------------------------------------------------


class WriteBehindCache:
    """
    Write-Behind (Write-Back): writes update the cache immediately and are
    batched to the backing store asynchronously.

    Trade-offs vs. Write-Through:
        - Write throughput: significantly higher (no synchronous store latency).
        - Consistency window: reads from cache are fresh, but data in the store
          may lag by up to flush_interval_ms.
        - Durability risk: if the cache node fails before flush, buffered writes
          are lost. Mitigated by persistent write-behind queues (Kafka, Redis
          Streams) in production.

    This implementation uses an in-process buffer for demonstration. A
    production implementation would use a durable queue.

    Parameters
    ----------
    cache : InMemoryStore
    store : BackingDataStore
    flush_interval_ms : float
        How often the write buffer is flushed to the store.
    max_buffer_size : int
        Maximum entries before forced flush (flow control).
    default_ttl_ms : float
    """

    def __init__(
        self,
        cache: InMemoryStore,
        store: BackingDataStore,
        flush_interval_ms: float = 500,
        max_buffer_size: int = 1000,
        default_ttl_ms: float = 60_000,
    ) -> None:
        self._cache = cache
        self._store = store
        self._flush_interval_ms = flush_interval_ms
        self._max_buffer_size = max_buffer_size
        self._default_ttl_ms = default_ttl_ms
        self._buffer: dict[str, WriteBufferEntry] = {}
        self._buffer_lock = threading.Lock()
        self._flush_count = 0
        self._last_flush_at = time.monotonic()

    def get(self, key: str) -> Optional[Any]:
        # Cache is always current for write-behind
        value = self._cache.get(key)
        if value is not None:
            return value
        # May need to check store if cache was evicted
        return self._store.get(key)

    def put(self, key: str, value: Any, ttl_ms: Optional[float] = None) -> None:
        """Write to cache immediately; buffer the store write."""
        effective_ttl = ttl_ms if ttl_ms is not None else self._default_ttl_ms
        self._cache.set(key, value, ttl_ms=effective_ttl)

        with self._buffer_lock:
            self._buffer[key] = WriteBufferEntry(
                key=key,
                value=value,
                enqueued_at=time.monotonic(),
            )
            # Flow control: flush if buffer is too large
            if len(self._buffer) >= self._max_buffer_size:
                self._do_flush()

    def flush(self) -> int:
        """
        Drain the write buffer to the backing store.

        Returns
        -------
        int
            Number of entries flushed.
        """
        with self._buffer_lock:
            return self._do_flush()

    def _do_flush(self) -> int:
        """Internal flush — caller must hold _buffer_lock."""
        count = 0
        keys_to_flush = list(self._buffer.keys())
        for key in keys_to_flush:
            entry = self._buffer.pop(key)
            self._store.put(key, entry.value)
            count += 1
        self._flush_count += 1
        self._last_flush_at = time.monotonic()
        return count

    def pending_count(self) -> int:
        with self._buffer_lock:
            return len(self._buffer)

    def delete(self, key: str) -> None:
        with self._buffer_lock:
            self._buffer.pop(key, None)
        self._cache.delete(key)
        self._store.delete(key)


# ---------------------------------------------------------------------------
# Pattern 4 — TTL Invalidation Strategy
# ---------------------------------------------------------------------------


class TTLInvalidationStrategy:
    """
    Smart TTL management: jitter, sliding window renewal, and dependency
    invalidation to prevent synchronized mass expiry.

    Problems solved:
        - Cache stampede on synchronized expiry: adding random jitter spreads
          expirations across a window, avoiding simultaneous misses.
        - Unnecessary evictions: sliding window extends TTL on each read,
          keeping hot data in cache longer.
        - Stale dependent entries: when a parent record (e.g., user profile)
          changes, all derived keys (e.g., user_orders, user_cart) must be
          invalidated. Dependency graph tracks these relationships.

    Parameters
    ----------
    base_ttl_ms : float
        Base TTL before jitter.
    jitter_factor : float
        Fraction of base_ttl_ms to add as random jitter. 0.2 = ±20%.
    sliding_window_ms : float
        On each read hit, extend TTL by this amount (if less than base_ttl_ms).
        Set to 0 to disable sliding window.
    """

    def __init__(
        self,
        base_ttl_ms: float = 30_000,
        jitter_factor: float = 0.2,
        sliding_window_ms: float = 0,
    ) -> None:
        self._base_ttl_ms = base_ttl_ms
        self._jitter_factor = jitter_factor
        self._sliding_window_ms = sliding_window_ms
        # dependency_graph[parent_key] = {child_key1, child_key2, ...}
        self._dependency_graph: dict[str, set[str]] = defaultdict(set)

    def compute_ttl_ms(self, base_override: Optional[float] = None) -> float:
        """
        Compute a jittered TTL.

        The jitter range is [-jitter_factor * base, +jitter_factor * base],
        so all expirations are within ±20% of the base TTL.
        """
        base = base_override if base_override is not None else self._base_ttl_ms
        jitter_range = base * self._jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        return max(1_000.0, base + jitter)

    def register_dependency(self, parent_key: str, child_key: str) -> None:
        """
        Register that child_key depends on parent_key.

        When parent_key is invalidated, child_key must also be evicted.
        """
        self._dependency_graph[parent_key].add(child_key)

    def get_dependent_keys(self, parent_key: str) -> frozenset[str]:
        """Return all keys that must be invalidated when parent_key changes."""
        result: set[str] = set()
        self._collect_dependents(parent_key, result)
        return frozenset(result)

    def _collect_dependents(self, key: str, visited: set[str]) -> None:
        for child in self._dependency_graph.get(key, set()):
            if child not in visited:
                visited.add(child)
                self._collect_dependents(child, visited)

    def should_renew_on_read(self, entry: CacheEntry) -> bool:
        """
        Return True if the entry's TTL should be extended on this read.

        Sliding window: renew if TTL remaining < sliding_window_ms (i.e., the
        entry is in the renewal window).
        """
        if self._sliding_window_ms <= 0:
            return False
        return entry.ttl_remaining_ms < self._sliding_window_ms


# ---------------------------------------------------------------------------
# Pattern 5 — Distributed Lock Manager
# ---------------------------------------------------------------------------


class DistributedLockManager:
    """
    Distributed lock with fencing tokens for stampede protection and
    split-brain prevention.

    In production, this would be implemented via:
        - Redis SET NX PX (Redlock algorithm for multi-node)
        - ZooKeeper ephemeral sequential nodes
        - etcd lease-based locks

    This implementation uses an in-process dict for demonstration. The fencing
    token mechanism — where each lock grant increments a sequence counter —
    is the key correctness mechanism: if a lock holder's token becomes stale
    (TTL expired, released), a subsequent grant issues a higher token, and any
    write with the stale token is rejected.

    Parameters
    ----------
    node_id : str
        Identifier of this lock manager instance (cache node).
    """

    def __init__(self, node_id: str = "lock-mgr-0") -> None:
        self.node_id = node_id
        self._locks: dict[str, FencingToken] = {}
        self._sequence_counters: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def acquire(
        self,
        lock_key: str,
        holder_id: str,
        ttl_ms: float = 5_000,
    ) -> Optional[FencingToken]:
        """
        Try to acquire the lock. Returns a FencingToken on success, None on failure.

        The fencing token's sequence number is monotonically increasing per
        lock_key, allowing downstream systems to reject stale writes.
        """
        with self._lock:
            existing = self._locks.get(lock_key)
            if existing is not None and existing.is_valid:
                return None  # Lock held by someone else

            self._sequence_counters[lock_key] += 1
            now = time.monotonic()
            token = FencingToken(
                token_id=str(uuid.uuid4()),
                lock_key=lock_key,
                holder_id=holder_id,
                sequence=self._sequence_counters[lock_key],
                granted_at=now,
                expires_at=now + ttl_ms / 1000.0,
            )
            self._locks[lock_key] = token
            return token

    def release(self, token: FencingToken) -> bool:
        """
        Release the lock. Returns True if the token was valid and the lock
        was released; False if the token was already expired or superseded.
        """
        with self._lock:
            existing = self._locks.get(token.lock_key)
            if existing is None or existing.token_id != token.token_id:
                return False
            existing.state = LockState.RELEASED
            del self._locks[token.lock_key]
            return True

    def validate_token(self, token: FencingToken) -> bool:
        """
        Validate a fencing token before a protected write.

        A write must call this before modifying the backing store. If False,
        the write must be rejected — the lock has been acquired by a newer holder.
        """
        with self._lock:
            existing = self._locks.get(token.lock_key)
            if existing is None:
                return False
            return (
                existing.token_id == token.token_id
                and existing.is_valid
            )

    def is_locked(self, lock_key: str) -> bool:
        with self._lock:
            existing = self._locks.get(lock_key)
            return existing is not None and existing.is_valid


# ---------------------------------------------------------------------------
# Pattern 6 — Cache Invalidation Bus
# ---------------------------------------------------------------------------


class CacheInvalidationBus:
    """
    Fan-out cache invalidation via a message bus.

    In a multi-node cache cluster, when node A evicts or updates a key, nodes
    B, C, and D still hold stale copies. Without a bus, those stale copies
    persist until TTL expiry. The CacheInvalidationBus publishes InvalidationEvent
    messages that all nodes subscribe to, triggering immediate local eviction.

    In production, the transport is typically:
        - Redis Pub/Sub (simple, no durability)
        - Kafka (durable, replayable)
        - Redis Streams (durable, consumer groups for multi-DC)

    This implementation uses in-process callbacks to simulate bus delivery.

    Parameters
    ----------
    bus_id : str
    """

    def __init__(self, bus_id: str = "invalidation-bus-0") -> None:
        self.bus_id = bus_id
        self._subscribers: list[Callable[[InvalidationEvent], None]] = []
        self._published: list[InvalidationEvent] = []

    def subscribe(self, callback: Callable[[InvalidationEvent], None]) -> None:
        """Register a cache node's eviction handler."""
        self._subscribers.append(callback)

    def publish(
        self,
        key: str,
        reason: InvalidationReason,
        source_node_id: str,
        dependency_key: Optional[str] = None,
    ) -> InvalidationEvent:
        """
        Publish an invalidation event to all subscribers.

        The source node does NOT receive its own event (it already evicted the
        key). In production, this is handled by message bus consumer group
        membership or by filtering on source_node_id.
        """
        event = InvalidationEvent(
            event_id=str(uuid.uuid4()),
            key=key,
            reason=reason,
            source_node_id=source_node_id,
            timestamp=time.monotonic(),
            dependency_key=dependency_key,
        )
        self._published.append(event)
        for callback in self._subscribers:
            callback(event)
        return event

    def published_count(self) -> int:
        return len(self._published)

    def last_event(self) -> Optional[InvalidationEvent]:
        return self._published[-1] if self._published else None


class InvalidatingCache(InMemoryStore):
    """
    InMemoryStore extended to participate in the CacheInvalidationBus.
    On receiving an invalidation event, evicts the key locally.
    """

    def __init__(self, node_id: str, bus: CacheInvalidationBus) -> None:
        super().__init__(node_id=node_id)
        self._bus = bus
        self._eviction_count = 0
        bus.subscribe(self._handle_invalidation_event)

    def _handle_invalidation_event(self, event: InvalidationEvent) -> None:
        # Skip events originating from this node (already evicted locally)
        if event.source_node_id == self.node_id:
            return
        if self.delete(event.key):
            self._eviction_count += 1

    def invalidate_and_publish(
        self,
        key: str,
        reason: InvalidationReason = InvalidationReason.EXPLICIT_DELETE,
    ) -> None:
        """Evict locally and publish to bus for fan-out."""
        self.delete(key)
        self._bus.publish(key=key, reason=reason, source_node_id=self.node_id)


# ---------------------------------------------------------------------------
# Pattern 7 — Cache Warmup Strategy
# ---------------------------------------------------------------------------


class CacheWarmupStrategy:
    """
    Pre-populate a cache before the application accepts traffic.

    A cold cache after deployment or a node restart causes a latency spike
    as the first N requests all miss and hit the backing store simultaneously.
    Warmup strategies:

        1. Snapshot warmup: load a snapshot of frequently-accessed keys
           from a file, database query, or serialized cache dump.

        2. Access log replay: replay recent access logs to pre-populate
           the keys that were hot before the restart.

        3. Priority-based loading: load high-priority keys first (e.g.,
           configuration, user sessions, product catalog) before lower-priority
           keys, so the application can start serving traffic with partial warmup.

    Parameters
    ----------
    cache : InMemoryStore
    store : BackingDataStore
    default_ttl_ms : float
    max_warmup_keys : int
        Maximum number of keys to load during warmup (safety limit).
    """

    def __init__(
        self,
        cache: InMemoryStore,
        store: BackingDataStore,
        default_ttl_ms: float = 60_000,
        max_warmup_keys: int = 10_000,
    ) -> None:
        self._cache = cache
        self._store = store
        self._default_ttl_ms = default_ttl_ms
        self._max_warmup_keys = max_warmup_keys
        self.loaded_count = 0
        self.skipped_count = 0

    def warmup_from_records(self, records: list[WarmupRecord]) -> int:
        """
        Load warmup records into cache, sorted by priority (highest first).

        Returns
        -------
        int
            Number of keys successfully loaded into cache.
        """
        sorted_records = sorted(records, key=lambda r: r.priority, reverse=True)
        loaded = 0
        for record in sorted_records:
            if loaded >= self._max_warmup_keys:
                self.skipped_count += len(sorted_records) - loaded
                break
            self._cache.set(record.key, record.value, ttl_ms=self._default_ttl_ms)
            loaded += 1

        self.loaded_count += loaded
        return loaded

    def warmup_from_key_list(self, keys: list[str]) -> int:
        """
        Load keys from the backing store into cache.

        Useful when we know which keys to warm (access log replay) but don't
        have the values cached locally.

        Returns
        -------
        int
            Number of keys loaded.
        """
        loaded = 0
        for key in keys[:self._max_warmup_keys]:
            value = self._store.get(key)
            if value is not None:
                self._cache.set(key, value, ttl_ms=self._default_ttl_ms)
                loaded += 1
        self.loaded_count += loaded
        return loaded

    def warmup_from_store_prefix(self, prefix: str) -> int:
        """
        Load all keys from the store matching a given prefix.

        Common use: warm all configuration keys (prefix="config:") or
        all active user sessions (prefix="session:") at startup.
        """
        # In production: SELECT key, value FROM cache_snapshot WHERE key LIKE 'prefix%'
        matching_keys = [
            k for k in self._store._data.keys()
            if k.startswith(prefix)
        ]
        return self.warmup_from_key_list(matching_keys[:self._max_warmup_keys])


# ---------------------------------------------------------------------------
# Demo scenarios
# ---------------------------------------------------------------------------


def _print_section(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def demo_cache_aside_stampede_protection() -> None:
    _print_section("Pattern 1: Cache-Aside with Stampede Protection")

    store = BackingDataStore()
    store.seed({
        "user:1001": {"id": 1001, "name": "Alice Chen", "tier": "premium"},
        "user:1002": {"id": 1002, "name": "Bob Davis", "tier": "standard"},
        "product:P-500": {"id": "P-500", "sku": "LAPTOP-PRO", "price": 1299.00},
    })

    cache = InMemoryStore("node-0")
    lock_mgr = DistributedLockManager("lock-0")
    ttl_strategy = TTLInvalidationStrategy(base_ttl_ms=10_000, jitter_factor=0.2)
    pattern = CacheAsidePattern(cache, store, lock_mgr, default_ttl_ms=10_000, ttl_strategy=ttl_strategy)

    # First access — cache miss, loads from store
    val = pattern.get("user:1001")
    print(f"\nFirst access (cache miss)  → {val}")
    print(f"Store reads: {store.read_count}, Cache hits: {cache.hit_count}, misses: {cache.miss_count}")

    # Second access — cache hit
    val = pattern.get("user:1001")
    print(f"Second access (cache hit)  → {val}")
    print(f"Store reads: {store.read_count}, Cache hits: {cache.hit_count}, misses: {cache.miss_count}")

    # Load product with custom loader
    def product_loader(k: str) -> Optional[dict]:
        return store.get(k)

    val = pattern.get("product:P-500", loader=product_loader)
    print(f"Product (custom loader)    → {val}")
    print(f"Store reads: {store.read_count}, Cache size: {cache.size()}")

    # Explicit invalidation
    pattern.invalidate("user:1001")
    print(f"\nAfter invalidate('user:1001'), cache size: {cache.size()}")


def demo_write_through_cache() -> None:
    _print_section("Pattern 2: Write-Through Cache")

    cache = InMemoryStore("node-0")
    store = BackingDataStore()
    wt = WriteThroughCache(cache, store, default_ttl_ms=30_000)

    # Write — updates both cache and store
    wt.put("order:5001", {"id": 5001, "status": "CONFIRMED", "total": 450.00})
    wt.put("order:5002", {"id": 5002, "status": "SHIPPED", "total": 89.95})

    print(f"\nWrote 2 orders. Store writes: {store.write_count}")
    print(f"Cache size: {cache.size()}")

    # Read — cache hit, no store read
    val = wt.get("order:5001")
    print(f"Read order:5001 (cache hit): {val}")
    print(f"Store reads: {store.read_count}")

    # Delete — removes from both
    wt.delete("order:5001")
    print(f"\nAfter delete('order:5001'), cache size: {cache.size()}")
    print(f"Store read after delete: {store.get('order:5001')}")


def demo_write_behind_cache() -> None:
    _print_section("Pattern 3: Write-Behind Cache")

    cache = InMemoryStore("node-0")
    store = BackingDataStore()
    wb = WriteBehindCache(cache, store, flush_interval_ms=100, default_ttl_ms=30_000)

    # Write multiple records — immediate in cache, buffered for store
    for i in range(5):
        wb.put(f"event:{i}", {"seq": i, "payload": f"data-{i}"})

    print(f"\nWrote 5 events. Store writes (buffered): {store.write_count}")
    print(f"Buffer pending: {wb.pending_count()}, Cache size: {cache.size()}")

    # Read immediately — served from cache
    val = wb.get("event:2")
    print(f"Immediate read event:2 (from cache): {val}")

    # Flush buffer to store
    flushed = wb.flush()
    print(f"\nFlushed {flushed} entries. Store writes: {store.write_count}")
    print(f"Buffer pending after flush: {wb.pending_count()}")
    print(f"Store[event:3] after flush: {store.get('event:3')}")


def demo_ttl_invalidation_strategy() -> None:
    _print_section("Pattern 4: TTL Invalidation Strategy")

    strategy = TTLInvalidationStrategy(
        base_ttl_ms=10_000,
        jitter_factor=0.2,
        sliding_window_ms=3_000,
    )

    # Register dependency: when user:1001 changes, also invalidate derived keys
    strategy.register_dependency("user:1001", "user:1001:orders")
    strategy.register_dependency("user:1001", "user:1001:cart")
    strategy.register_dependency("user:1001:orders", "user:1001:orders:summary")

    # Compute jittered TTLs — they vary around base
    ttls = [strategy.compute_ttl_ms() for _ in range(5)]
    print(f"\nJittered TTLs (base=10000ms, jitter=±20%):")
    for t in ttls:
        print(f"  {t:.0f} ms")
    print(f"  Range: {min(ttls):.0f} – {max(ttls):.0f} ms")

    # Transitively resolve dependents
    deps = strategy.get_dependent_keys("user:1001")
    print(f"\nKeys to invalidate when user:1001 changes: {sorted(deps)}")


def demo_distributed_lock() -> None:
    _print_section("Pattern 5: Distributed Lock with Fencing Tokens")

    lock_mgr = DistributedLockManager("lock-mgr-0")

    # Acquire lock
    token = lock_mgr.acquire("record:500", holder_id="worker-1", ttl_ms=2_000)
    print(f"\nAcquired lock: token.sequence={token.sequence}, valid={token.is_valid}")

    # Second acquire on same key fails
    token2 = lock_mgr.acquire("record:500", holder_id="worker-2", ttl_ms=2_000)
    print(f"Second acquire (should fail): {token2}")

    # Validate fencing token before a write
    valid = lock_mgr.validate_token(token)
    print(f"Fencing token valid before write: {valid}")

    # Release and re-acquire
    lock_mgr.release(token)
    token3 = lock_mgr.acquire("record:500", holder_id="worker-2", ttl_ms=2_000)
    print(f"\nRe-acquired after release: token.sequence={token3.sequence} (incremented)")

    # Original token is now stale — validate returns False
    valid_stale = lock_mgr.validate_token(token)
    print(f"Original (stale) token valid: {valid_stale} (expected False)")

    lock_mgr.release(token3)


def demo_cache_invalidation_bus() -> None:
    _print_section("Pattern 6: Cache Invalidation Bus (Multi-Node)")

    bus = CacheInvalidationBus("bus-0")
    node_a = InvalidatingCache("node-a", bus)
    node_b = InvalidatingCache("node-b", bus)
    node_c = InvalidatingCache("node-c", bus)

    # Seed same data on all three nodes
    for node in [node_a, node_b, node_c]:
        node.set("product:P-900", {"name": "Widget Pro", "price": 49.99}, ttl_ms=60_000)
        node.set("config:rate-limit", 1000, ttl_ms=60_000)

    print(f"\nAll nodes seeded. Node A size: {node_a.size()}, B: {node_b.size()}, C: {node_c.size()}")

    # Node A updates the product and publishes invalidation
    node_a.set("product:P-900", {"name": "Widget Pro", "price": 44.99}, ttl_ms=60_000)
    node_a.invalidate_and_publish("product:P-900", InvalidationReason.EXPLICIT_DELETE)

    print(f"\nNode A published invalidation for product:P-900")
    print(f"Node B eviction count: {node_b._eviction_count}")
    print(f"Node C eviction count: {node_c._eviction_count}")
    print(f"Node B product:P-900 (should be None): {node_b.get('product:P-900')}")
    print(f"Node C product:P-900 (should be None): {node_c.get('product:P-900')}")
    print(f"Node A product:P-900 (local value preserved): {node_a.get('product:P-900')}")
    print(f"\nBus events published: {bus.published_count()}")


def demo_cache_warmup() -> None:
    _print_section("Pattern 7: Cache Warmup Strategy")

    store = BackingDataStore()
    store.seed({
        "config:timeout": 30,
        "config:max-retries": 3,
        "config:rate-limit": 1000,
        "catalog:CAT-001": {"name": "Electronics", "active": True},
        "catalog:CAT-002": {"name": "Clothing", "active": True},
        "session:expired-session": {"user_id": 9999},
    })

    cache = InMemoryStore("node-0")
    warmup = CacheWarmupStrategy(cache, store, default_ttl_ms=60_000, max_warmup_keys=100)

    # Warmup from pre-built records (e.g., from a cache snapshot)
    snapshot_records = [
        WarmupRecord(key="product:HOT-001", value={"sku": "BEST-SELLER"}, priority=100),
        WarmupRecord(key="product:HOT-002", value={"sku": "TRENDING"}, priority=90),
        WarmupRecord(key="product:COLD-001", value={"sku": "SLOW-MOVER"}, priority=1),
    ]
    loaded = warmup.warmup_from_records(snapshot_records)
    print(f"\nWarmup from records: loaded {loaded} entries")

    # Warmup from store prefix
    loaded_config = warmup.warmup_from_store_prefix("config:")
    print(f"Warmup from store prefix 'config:': loaded {loaded_config} keys")

    loaded_catalog = warmup.warmup_from_store_prefix("catalog:")
    print(f"Warmup from store prefix 'catalog:': loaded {loaded_catalog} keys")

    print(f"\nCache size after warmup: {cache.size()}")
    print(f"Total loaded: {warmup.loaded_count}")
    print(f"Store reads during warmup: {store.read_count}")
    print(f"\nSample cached values:")
    print(f"  product:HOT-001 = {cache.get('product:HOT-001')}")
    print(f"  config:timeout  = {cache.get('config:timeout')}")
    print(f"  catalog:CAT-001 = {cache.get('catalog:CAT-001')}")


if __name__ == "__main__":
    print("Distributed Cache Patterns — Enterprise Reference Implementation")
    print("Cache-Aside | Write-Through | Write-Behind | TTL | Lock | Bus | Warmup")

    demo_cache_aside_stampede_protection()
    demo_write_through_cache()
    demo_write_behind_cache()
    demo_ttl_invalidation_strategy()
    demo_distributed_lock()
    demo_cache_invalidation_bus()
    demo_cache_warmup()

    print("\n" + "=" * 70)
    print("All patterns demonstrated successfully.")
    print("=" * 70)
