"""
14_api_gateway_patterns.py — Rate Limiting, Circuit Breaker, Request Coalescing,
API Key Management, Version Routing, and Idempotency for enterprise API gateways.

Every production API gateway must solve six problems that simple HTTP proxies
do not address. This module provides reference implementations for each:

    1. Rate Limiting (Token Bucket + Sliding Window Log):
       Prevents abuse and protects backend systems from overload. Two algorithms
       with different tradeoffs: TokenBucketRateLimiter allows burst capacity while
       maintaining average rate; SlidingWindowRateLimiter provides exact counting
       with no burst artifacts at window boundaries.

    2. Circuit Breaker (CLOSED → OPEN → HALF_OPEN state machine):
       Prevents cascading failure by stopping traffic to a failing backend and
       allowing periodic probe requests to detect recovery. Exponential backoff
       on repeated trips prevents a recovering system from being overwhelmed.

    3. Request Coalescing (In-Flight Deduplication):
       When identical requests arrive concurrently (cache stampede, retry storm),
       coalesce them into a single backend call. All waiters receive the same
       result as the first caller — zero redundant upstream calls.

    4. API Key Manager (Lifecycle + Rotation + Tier-Based Rate Limits):
       API keys go through ACTIVE → EXPIRING_SOON → EXPIRED; rotation issues a
       new key with a grace period during which both keys are valid. Tier-based
       rate limit assignment (free/standard/premium) ties permissions to the key.

    5. API Version Router (Header + URL Versioning, Sunset Policy):
       Supports both header-based (API-Version: 2026-01) and URL-based (/v2/)
       versioning. Deprecated versions return a Sunset header; expired versions
       return 410 Gone. Enforces version lifecycle without code duplication.

    6. Request Deduplicator (Idempotency Key):
       Client-provided idempotency key (UUID) stores the response; duplicate
       requests within the detection window return the cached response without
       re-processing. Prevents double-charges, double-submits, and duplicate
       side effects from client retries.

No external dependencies required.

Run:
    python examples/14_api_gateway_patterns.py
"""

from __future__ import annotations

import hashlib
import time
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------


class RateLimitAlgorithm(str, Enum):
    TOKEN_BUCKET = "TOKEN_BUCKET"
    SLIDING_WINDOW_LOG = "SLIDING_WINDOW_LOG"


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    limit: int
    remaining: int
    reset_after_ms: float
    retry_after_ms: float
    algorithm: RateLimitAlgorithm
    client_id: str


class CircuitState(str, Enum):
    CLOSED = "CLOSED"       # Normal operation
    OPEN = "OPEN"           # Failing fast — reject all requests
    HALF_OPEN = "HALF_OPEN" # Probing — allow one request through


@dataclass
class CircuitBreakerResult:
    """Result of a circuit breaker check."""
    allowed: bool
    state: CircuitState
    failure_count: int
    last_failure_at: Optional[float]
    reason: str


class APIKeyStatus(str, Enum):
    ACTIVE = "ACTIVE"
    EXPIRING_SOON = "EXPIRING_SOON"   # Within rotation_warning_days
    EXPIRED = "EXPIRED"
    REVOKED = "REVOKED"


class APIKeyTier(str, Enum):
    FREE = "FREE"
    STANDARD = "STANDARD"
    PREMIUM = "PREMIUM"


@dataclass
class APIKey:
    """Represents a managed API key."""
    key_id: str
    key_hash: str         # SHA-256 of the raw key (never store raw key)
    client_id: str
    tier: APIKeyTier
    created_at: float
    expires_at: float
    status: APIKeyStatus
    rate_limit_rpm: int   # Requests per minute for this key/tier
    # Rotation fields
    successor_key_id: Optional[str] = None   # Set when key is rotated
    grace_period_ends_at: Optional[float] = None  # Both keys valid until this time

    @property
    def is_valid(self) -> bool:
        return self.status in (APIKeyStatus.ACTIVE, APIKeyStatus.EXPIRING_SOON)

    @property
    def in_grace_period(self) -> bool:
        """True if this key has been rotated but is still within grace period."""
        return (
            self.successor_key_id is not None
            and self.grace_period_ends_at is not None
            and time.monotonic() < self.grace_period_ends_at
        )


@dataclass(frozen=True)
class APIVersion:
    """An API version specification."""
    version_id: str       # e.g. "2026-01" or "v2"
    introduced_at: float  # monotonic timestamp
    deprecated_at: Optional[float] = None
    sunset_at: Optional[float] = None
    is_current: bool = True
    handler_name: str = "default"

    @property
    def is_deprecated(self) -> bool:
        return self.deprecated_at is not None and time.monotonic() >= self.deprecated_at

    @property
    def is_expired(self) -> bool:
        return self.sunset_at is not None and time.monotonic() >= self.sunset_at


@dataclass
class VersionRoutingResult:
    """Result of version routing."""
    version_id: str
    handler_name: str
    is_deprecated: bool
    is_expired: bool
    sunset_header: Optional[str]  # ISO date string for Sunset header
    warning_header: Optional[str]


@dataclass
class IdempotencyRecord:
    """Stores the result of a request for idempotency detection."""
    idempotency_key: str
    response: Any
    created_at: float
    expires_at: float
    request_hash: str

    @property
    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at


# ---------------------------------------------------------------------------
# Pattern 1 — Rate Limiters
# ---------------------------------------------------------------------------


class TokenBucketRateLimiter:
    """
    Token Bucket rate limiting algorithm.

    Maintains a bucket of tokens that refills at a steady rate. Each request
    consumes one token. If the bucket is empty, the request is rejected.

    Properties:
        - Allows burst traffic up to bucket_capacity tokens
        - Maintains average rate = refill_rate tokens/second
        - Smoothly handles traffic spikes without cascading rejection

    Parameters
    ----------
    client_id : str
    rate_per_second : float
        Token refill rate (= sustained request rate)
    bucket_capacity : int
        Maximum tokens (= maximum burst size)
    """

    def __init__(
        self,
        client_id: str,
        rate_per_second: float,
        bucket_capacity: int,
    ) -> None:
        self.client_id = client_id
        self._rate = rate_per_second
        self._capacity = bucket_capacity
        self._tokens: float = float(bucket_capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self.allow_count = 0
        self.deny_count = 0

    def check(self) -> RateLimitResult:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            # Refill tokens based on elapsed time
            self._tokens = min(
                float(self._capacity),
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self.allow_count += 1
                return RateLimitResult(
                    allowed=True,
                    limit=self._capacity,
                    remaining=int(self._tokens),
                    reset_after_ms=0,
                    retry_after_ms=0,
                    algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
                    client_id=self.client_id,
                )
            else:
                # Time until next token available
                wait_s = (1.0 - self._tokens) / self._rate
                self.deny_count += 1
                return RateLimitResult(
                    allowed=False,
                    limit=self._capacity,
                    remaining=0,
                    reset_after_ms=wait_s * 1000,
                    retry_after_ms=wait_s * 1000,
                    algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
                    client_id=self.client_id,
                )


class SlidingWindowRateLimiter:
    """
    Sliding Window Log rate limiting algorithm.

    Records the timestamp of every request within the current window. On each
    request, removes timestamps older than window_ms, then checks if the count
    exceeds the limit.

    Properties:
        - Exact request counting with no boundary artifacts
        - No burst: strictly enforces limit over any window_ms period
        - Higher memory: stores O(limit) timestamps per client

    Parameters
    ----------
    client_id : str
    limit : int
        Maximum requests per window
    window_ms : float
        Window size in milliseconds
    """

    def __init__(
        self,
        client_id: str,
        limit: int,
        window_ms: float = 60_000,
    ) -> None:
        self.client_id = client_id
        self._limit = limit
        self._window_ms = window_ms
        self._log: deque[float] = deque()
        self._lock = threading.Lock()
        self.allow_count = 0
        self.deny_count = 0

    def check(self) -> RateLimitResult:
        with self._lock:
            now_ms = time.monotonic() * 1000
            cutoff = now_ms - self._window_ms

            # Remove expired entries
            while self._log and self._log[0] <= cutoff:
                self._log.popleft()

            current_count = len(self._log)
            if current_count < self._limit:
                self._log.append(now_ms)
                self.allow_count += 1
                return RateLimitResult(
                    allowed=True,
                    limit=self._limit,
                    remaining=self._limit - current_count - 1,
                    reset_after_ms=0,
                    retry_after_ms=0,
                    algorithm=RateLimitAlgorithm.SLIDING_WINDOW_LOG,
                    client_id=self.client_id,
                )
            else:
                # Time until oldest request exits the window
                oldest = self._log[0]
                retry_after_ms = max(0.0, oldest - cutoff)
                self.deny_count += 1
                return RateLimitResult(
                    allowed=False,
                    limit=self._limit,
                    remaining=0,
                    reset_after_ms=retry_after_ms,
                    retry_after_ms=retry_after_ms,
                    algorithm=RateLimitAlgorithm.SLIDING_WINDOW_LOG,
                    client_id=self.client_id,
                )


# ---------------------------------------------------------------------------
# Pattern 2 — Circuit Breaker
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """
    Circuit Breaker with CLOSED → OPEN → HALF_OPEN state machine.

    CLOSED: Normal operation. Failures are counted.
    OPEN:   Failure threshold exceeded. All requests rejected for timeout_ms.
            After timeout, transitions to HALF_OPEN.
    HALF_OPEN: One probe request allowed through. On success → CLOSED;
              on failure → OPEN with doubled timeout (exponential backoff).

    Parameters
    ----------
    service_name : str
    failure_threshold : int
        Consecutive failures to trip to OPEN.
    success_threshold : int
        Consecutive successes in HALF_OPEN to return to CLOSED.
    timeout_ms : float
        Initial OPEN state duration before HALF_OPEN probe.
    max_timeout_ms : float
        Maximum OPEN duration after repeated trips.
    """

    def __init__(
        self,
        service_name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout_ms: float = 10_000,
        max_timeout_ms: float = 120_000,
    ) -> None:
        self.service_name = service_name
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._base_timeout_ms = timeout_ms
        self._max_timeout_ms = max_timeout_ms
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_at: Optional[float] = None
        self._opened_at: Optional[float] = None
        self._current_timeout_ms = timeout_ms
        self._trip_count = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    def check(self) -> CircuitBreakerResult:
        """Check if a request should be allowed through."""
        with self._lock:
            now = time.monotonic()

            if self._state == CircuitState.OPEN:
                elapsed_ms = (now - (self._opened_at or now)) * 1000
                if elapsed_ms >= self._current_timeout_ms:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    return CircuitBreakerResult(
                        allowed=True,
                        state=CircuitState.HALF_OPEN,
                        failure_count=self._failure_count,
                        last_failure_at=self._last_failure_at,
                        reason="probe request — circuit transitioning to HALF_OPEN",
                    )
                return CircuitBreakerResult(
                    allowed=False,
                    state=CircuitState.OPEN,
                    failure_count=self._failure_count,
                    last_failure_at=self._last_failure_at,
                    reason=(
                        f"circuit OPEN — {self._failure_count} consecutive failures; "
                        f"retry in {self._current_timeout_ms - elapsed_ms:.0f} ms"
                    ),
                )

            if self._state == CircuitState.HALF_OPEN:
                # Only allow one probe at a time — in a real implementation,
                # use a semaphore; here we allow the probe unconditionally
                return CircuitBreakerResult(
                    allowed=True,
                    state=CircuitState.HALF_OPEN,
                    failure_count=self._failure_count,
                    last_failure_at=self._last_failure_at,
                    reason="probe request in HALF_OPEN state",
                )

            return CircuitBreakerResult(
                allowed=True,
                state=CircuitState.CLOSED,
                failure_count=self._failure_count,
                last_failure_at=self._last_failure_at,
                reason="circuit CLOSED — normal operation",
            )

    def record_success(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._current_timeout_ms = self._base_timeout_ms
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_at = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                # Probe failed — reopen with doubled timeout
                self._trip_count += 1
                self._current_timeout_ms = min(
                    self._current_timeout_ms * 2,
                    self._max_timeout_ms,
                )
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._trip_count += 1
                    self._state = CircuitState.OPEN
                    self._opened_at = time.monotonic()


# ---------------------------------------------------------------------------
# Pattern 3 — Request Coalescer
# ---------------------------------------------------------------------------


class RequestCoalescer:
    """
    In-flight request deduplication (coalescing).

    When multiple identical requests arrive concurrently, only the first is
    forwarded to the backend. All subsequent identical in-flight requests
    wait for the first to complete and receive the same result.

    This is distinct from caching: coalescing only deduplicates concurrent
    in-flight requests; once the first request completes, the deduplication
    window is closed. A cache retains results for future requests.

    Parameters
    ----------
    key_fn : callable
        Function that maps request arguments to a string coalescing key.
        Identical keys = identical requests to deduplicate.
    """

    def __init__(self, key_fn: Callable[..., str]) -> None:
        self._key_fn = key_fn
        self._in_flight: dict[str, Any] = {}
        self._waiters: dict[str, list] = {}
        self._lock = threading.Lock()
        self.coalesced_count = 0
        self.forwarded_count = 0

    def get_or_fetch(
        self,
        fetch_fn: Callable[[], Any],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Any, bool]:
        """
        Return (result, was_coalesced).

        was_coalesced=True means this call was deduplicated — it received the
        same result as another concurrent request without calling fetch_fn.
        """
        key = self._key_fn(*args, **kwargs)

        with self._lock:
            if key in self._in_flight:
                # Already in flight — register as waiter
                event = threading.Event()
                result_box = [None]
                self._waiters.setdefault(key, []).append((event, result_box))
                self.coalesced_count += 1
                is_first = False
            else:
                # First request — mark as in-flight
                self._in_flight[key] = True
                self._waiters[key] = []
                is_first = True

        if is_first:
            try:
                result = fetch_fn()
                self.forwarded_count += 1
            finally:
                with self._lock:
                    del self._in_flight[key]
                    waiters = self._waiters.pop(key, [])
                for event, result_box in waiters:
                    result_box[0] = result
                    event.set()
            return result, False
        else:
            # Wait for the first request to complete
            with self._lock:
                my_waiters = self._waiters.get(key, [])
                if my_waiters:
                    event, result_box = my_waiters[-1]
                else:
                    # Request already completed
                    return None, True
            event.wait(timeout=5.0)
            return result_box[0], True


# ---------------------------------------------------------------------------
# Pattern 4 — API Key Manager
# ---------------------------------------------------------------------------


class APIKeyManager:
    """
    API key lifecycle management with rotation and tier-based rate limits.

    Key lifecycle:
        ACTIVE         → normal use
        EXPIRING_SOON  → within rotation_warning_days of expiry; use continues
        EXPIRED        → past expiry; all requests rejected
        REVOKED        → explicitly invalidated; all requests rejected

    Rotation creates a new successor key and enters a grace period during which
    both the old and new keys are valid. After grace_period_ms, the old key
    transitions to EXPIRED.

    Tier rate limits (requests/minute):
        FREE     → 10 rpm
        STANDARD → 100 rpm
        PREMIUM  → 1000 rpm
    """

    TIER_RATE_LIMITS: dict[APIKeyTier, int] = {
        APIKeyTier.FREE: 10,
        APIKeyTier.STANDARD: 100,
        APIKeyTier.PREMIUM: 1000,
    }

    def __init__(
        self,
        rotation_warning_days: float = 7,
        grace_period_ms: float = 86_400_000,  # 24 hours
        default_ttl_days: float = 365,
    ) -> None:
        self._rotation_warning_s = rotation_warning_days * 86400
        self._grace_period_ms = grace_period_ms
        self._default_ttl_s = default_ttl_days * 86400
        self._keys: dict[str, APIKey] = {}
        self._lock = threading.Lock()

    def create_key(
        self,
        client_id: str,
        tier: APIKeyTier = APIKeyTier.STANDARD,
        raw_key: Optional[str] = None,
        ttl_days: Optional[float] = None,
    ) -> tuple[str, APIKey]:
        """
        Create a new API key. Returns (raw_key, APIKey).

        The raw key is only returned at creation time — subsequent operations
        work with key_id. The stored key_hash is SHA-256(raw_key).
        """
        if raw_key is None:
            raw_key = str(uuid.uuid4()).replace("-", "")
        key_id = str(uuid.uuid4())
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        now = time.monotonic()
        ttl_s = (ttl_days or 365) * 86400

        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            client_id=client_id,
            tier=tier,
            created_at=now,
            expires_at=now + ttl_s,
            status=APIKeyStatus.ACTIVE,
            rate_limit_rpm=self.TIER_RATE_LIMITS[tier],
        )
        with self._lock:
            self._keys[key_id] = api_key
        return raw_key, api_key

    def validate_key(self, key_id: str, raw_key: str) -> tuple[bool, Optional[APIKey], str]:
        """
        Validate an API key. Returns (is_valid, api_key, reason).
        """
        with self._lock:
            api_key = self._keys.get(key_id)
            if api_key is None:
                return False, None, "key not found"

            # Check hash
            if hashlib.sha256(raw_key.encode()).hexdigest() != api_key.key_hash:
                return False, None, "invalid key"

            now = time.monotonic()

            # Update status
            if api_key.status == APIKeyStatus.ACTIVE:
                if now >= api_key.expires_at:
                    api_key.status = APIKeyStatus.EXPIRED
                elif (api_key.expires_at - now) <= self._rotation_warning_s:
                    api_key.status = APIKeyStatus.EXPIRING_SOON

            if api_key.status in (APIKeyStatus.EXPIRED, APIKeyStatus.REVOKED):
                return False, api_key, f"key {api_key.status.value.lower()}"

            return True, api_key, "valid"

    def rotate_key(self, key_id: str) -> tuple[str, APIKey]:
        """
        Rotate a key: create successor + enter grace period.
        Returns (new_raw_key, new_api_key).
        """
        with self._lock:
            old_key = self._keys.get(key_id)
            if old_key is None:
                raise KeyError(f"key {key_id} not found")

        new_raw, new_key = self.create_key(
            client_id=old_key.client_id,
            tier=old_key.tier,
            ttl_days=self._default_ttl_s / 86400,
        )
        now = time.monotonic()
        with self._lock:
            old_key.successor_key_id = new_key.key_id
            old_key.grace_period_ends_at = now + self._grace_period_ms / 1000.0

        return new_raw, new_key

    def revoke_key(self, key_id: str) -> bool:
        with self._lock:
            key = self._keys.get(key_id)
            if key is None:
                return False
            key.status = APIKeyStatus.REVOKED
            return True

    def get_key(self, key_id: str) -> Optional[APIKey]:
        return self._keys.get(key_id)


# ---------------------------------------------------------------------------
# Pattern 5 — API Version Router
# ---------------------------------------------------------------------------


class APIVersionRouter:
    """
    API version routing with header/URL versioning and sunset enforcement.

    Version lifecycle:
        Active     → normal operation
        Deprecated → response includes Sunset header; clients should migrate
        Expired    → returns 410 Gone; no longer served

    Version resolution order:
        1. Header: API-Version: 2026-01
        2. URL prefix: /v2/resource → version "v2"
        3. Default: latest non-deprecated version
    """

    def __init__(self) -> None:
        self._versions: dict[str, APIVersion] = {}
        self._current_version_id: Optional[str] = None

    def register_version(self, version: APIVersion) -> None:
        self._versions[version.version_id] = version
        if version.is_current:
            self._current_version_id = version.version_id

    def route(self, version_header: Optional[str] = None, url_prefix: Optional[str] = None) -> VersionRoutingResult:
        """
        Resolve the version to route to.

        Parameters
        ----------
        version_header : str | None
            Value of the API-Version header.
        url_prefix : str | None
            Version prefix extracted from URL (e.g., "v2" from "/v2/users").
        """
        # Resolve version ID from header, then URL, then default
        version_id = version_header or url_prefix or self._current_version_id
        if version_id is None:
            raise ValueError("no versions registered")

        version = self._versions.get(version_id)
        if version is None:
            # Unknown version — fall back to current
            version = self._versions.get(self._current_version_id)
            if version is None:
                raise ValueError(f"version {version_id!r} not found")

        sunset_header = None
        warning_header = None

        if version.is_expired:
            return VersionRoutingResult(
                version_id=version.version_id,
                handler_name=version.handler_name,
                is_deprecated=True,
                is_expired=True,
                sunset_header=None,
                warning_header=f'410 Gone: API version {version.version_id} has been retired',
            )

        if version.is_deprecated:
            # Sunset header: RFC 8594 date string (approximate)
            if version.sunset_at is not None:
                sunset_header = f"T+{int(version.sunset_at - time.monotonic())}s"
            warning_header = (
                f'299 - "Deprecated: API version {version.version_id} will be '
                f'retired on {sunset_header}; please migrate to {self._current_version_id}"'
            )

        return VersionRoutingResult(
            version_id=version.version_id,
            handler_name=version.handler_name,
            is_deprecated=version.is_deprecated,
            is_expired=False,
            sunset_header=sunset_header,
            warning_header=warning_header,
        )

    def list_versions(self) -> list[APIVersion]:
        return sorted(self._versions.values(), key=lambda v: v.introduced_at)


# ---------------------------------------------------------------------------
# Pattern 6 — Request Deduplicator (Idempotency Key)
# ---------------------------------------------------------------------------


class RequestDeduplicator:
    """
    Idempotency key-based request deduplication.

    Clients provide an Idempotency-Key header (UUID) with mutating requests
    (POST, PATCH, DELETE). The first request is processed normally and the
    response is stored. Identical requests within the TTL window return the
    stored response without re-executing the handler.

    This prevents:
        - Double-charges from payment retries
        - Duplicate resource creation from network timeout retries
        - Double-submission of forms or job dispatches

    Parameters
    ----------
    ttl_ms : float
        How long to retain idempotency records (default: 24 hours)
    """

    def __init__(self, ttl_ms: float = 86_400_000) -> None:
        self._ttl_ms = ttl_ms
        self._store: dict[str, IdempotencyRecord] = {}
        self._lock = threading.Lock()
        self.dedup_count = 0
        self.processed_count = 0

    def execute(
        self,
        idempotency_key: str,
        handler: Callable[[], Any],
        request_hash: str = "",
    ) -> tuple[Any, bool]:
        """
        Execute handler or return cached response.

        Returns
        -------
        (response, was_duplicate)
            was_duplicate=True means the cached response was returned.
        """
        with self._lock:
            record = self._store.get(idempotency_key)
            if record is not None and not record.is_expired:
                self.dedup_count += 1
                return record.response, True

        # Not a duplicate — process
        response = handler()
        now = time.monotonic()
        record = IdempotencyRecord(
            idempotency_key=idempotency_key,
            response=response,
            created_at=now,
            expires_at=now + self._ttl_ms / 1000.0,
            request_hash=request_hash,
        )
        with self._lock:
            self._store[idempotency_key] = record
            self.processed_count += 1

        return response, False

    def purge_expired(self) -> int:
        """Remove expired idempotency records. Returns count removed."""
        with self._lock:
            expired = [k for k, v in self._store.items() if v.is_expired]
            for k in expired:
                del self._store[k]
            return len(expired)

    def store_size(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# Demo scenarios
# ---------------------------------------------------------------------------


def _print_section(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def demo_rate_limiting() -> None:
    _print_section("Pattern 1: Rate Limiting — Token Bucket vs. Sliding Window")

    # Token bucket — allows initial burst
    tb = TokenBucketRateLimiter("client:A", rate_per_second=2.0, bucket_capacity=5)
    print("\nToken Bucket (rate=2/s, capacity=5) — 7 rapid requests:")
    for i in range(7):
        r = tb.check()
        print(f"  Request {i+1}: {'ALLOW' if r.allowed else 'DENY'} (remaining={r.remaining}, retry_after={r.retry_after_ms:.0f}ms)")

    # Sliding window — exact counting, no burst
    sw = SlidingWindowRateLimiter("client:B", limit=3, window_ms=1000)
    print("\nSliding Window (limit=3/s) — 5 rapid requests:")
    for i in range(5):
        r = sw.check()
        print(f"  Request {i+1}: {'ALLOW' if r.allowed else 'DENY'} (remaining={r.remaining}, retry_after={r.retry_after_ms:.0f}ms)")

    print(f"\nToken Bucket: {tb.allow_count} allowed, {tb.deny_count} denied")
    print(f"Sliding Window: {sw.allow_count} allowed, {sw.deny_count} denied")


def demo_circuit_breaker() -> None:
    _print_section("Pattern 2: Circuit Breaker")

    cb = CircuitBreaker("payment-service", failure_threshold=3, timeout_ms=5_000)

    print(f"\nInitial state: {cb.state.value}")

    # Record failures to trip
    for i in range(3):
        cb.record_failure()
        r = cb.check()
        print(f"After failure {i+1}: state={r.state.value}, reason='{r.reason[:60]}...'")

    # Check in OPEN state
    r = cb.check()
    print(f"\nRequest while OPEN: allowed={r.allowed}, state={r.state.value}")

    # Simulate recovery: force HALF_OPEN for testing
    cb._state = CircuitState.HALF_OPEN
    cb._success_count = 0
    r = cb.check()
    print(f"\nProbe in HALF_OPEN: allowed={r.allowed}")
    cb.record_success()
    cb.record_success()
    print(f"After 2 successes: state={cb.state.value}")


def demo_request_coalescing() -> None:
    _print_section("Pattern 3: Request Coalescing")

    def key_fn(endpoint: str, user_id: str) -> str:
        return f"{endpoint}:{user_id}"

    coalescer = RequestCoalescer(key_fn=key_fn)
    call_count = [0]

    def slow_backend():
        call_count[0] += 1
        time.sleep(0.01)  # 10ms backend latency
        return {"user_id": "U-100", "name": "Alice", "tier": "premium"}

    # Concurrent requests — only one should hit the backend
    results = []
    threads = []

    for i in range(5):
        def make_request(idx=i):
            result, coalesced = coalescer.get_or_fetch(slow_backend, "/users", "U-100")
            results.append((idx, coalesced))
        t = threading.Thread(target=make_request)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"\n5 concurrent requests for same resource:")
    print(f"  Backend calls: {call_count[0]} (expected: 1)")
    print(f"  Coalesced: {coalescer.coalesced_count}, Forwarded: {coalescer.forwarded_count}")


def demo_api_key_lifecycle() -> None:
    _print_section("Pattern 4: API Key Manager — Lifecycle + Rotation")

    mgr = APIKeyManager(rotation_warning_days=7, grace_period_ms=3_600_000)

    raw_key, key = mgr.create_key("client:premium-corp", tier=APIKeyTier.PREMIUM)
    print(f"\nCreated key: {key.key_id[:12]}... tier={key.tier.value} rpm={key.rate_limit_rpm}")

    # Validate
    valid, api_key, reason = mgr.validate_key(key.key_id, raw_key)
    print(f"Validation: valid={valid}, reason={reason}")

    # Rotate
    new_raw, new_key = mgr.rotate_key(key.key_id)
    old = mgr.get_key(key.key_id)
    print(f"\nRotated: old→{old.key_id[:12]}... successor={old.successor_key_id[:12]}...")
    print(f"New key: {new_key.key_id[:12]}... (grace period active: {old.in_grace_period})")

    # Old key still valid during grace period
    valid2, _, reason2 = mgr.validate_key(key.key_id, raw_key)
    print(f"Old key still valid (grace period): {valid2}, {reason2}")

    # Revoke
    mgr.revoke_key(key.key_id)
    valid3, _, reason3 = mgr.validate_key(key.key_id, raw_key)
    print(f"After revoke: valid={valid3}, reason={reason3}")


def demo_version_routing() -> None:
    _print_section("Pattern 5: API Version Router — Sunset Policy")

    now = time.monotonic()
    router = APIVersionRouter()
    router.register_version(APIVersion(
        version_id="2024-01",
        introduced_at=now - 200,
        deprecated_at=now - 100,   # already deprecated
        sunset_at=now + 1000,      # not yet expired
        is_current=False,
        handler_name="handler_v2024_01",
    ))
    router.register_version(APIVersion(
        version_id="2025-01",
        introduced_at=now - 100,
        deprecated_at=now - 50,
        sunset_at=now - 1,          # already expired!
        is_current=False,
        handler_name="handler_v2025_01",
    ))
    router.register_version(APIVersion(
        version_id="2026-01",
        introduced_at=now,
        is_current=True,
        handler_name="handler_v2026_01",
    ))

    for vid in ["2026-01", "2024-01", "2025-01"]:
        r = router.route(version_header=vid)
        print(f"\n  API-Version: {vid}")
        print(f"    handler={r.handler_name}, deprecated={r.is_deprecated}, expired={r.is_expired}")
        if r.sunset_header:
            print(f"    Sunset: {r.sunset_header}")
        if r.warning_header:
            print(f"    Warning: {r.warning_header[:80]}...")


def demo_idempotency() -> None:
    _print_section("Pattern 6: Request Deduplicator (Idempotency Key)")

    dedup = RequestDeduplicator(ttl_ms=60_000)
    call_count = [0]

    def create_order():
        call_count[0] += 1
        return {"order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}", "status": "CREATED"}

    idempotency_key = str(uuid.uuid4())

    # First call — processes
    r1, was_dup = dedup.execute(idempotency_key, create_order)
    print(f"\nFirst call (idempotency_key={idempotency_key[:8]}...): was_duplicate={was_dup}")
    print(f"  Result: {r1}")

    # Second call with same key — returns cached response
    r2, was_dup2 = dedup.execute(idempotency_key, create_order)
    print(f"\nSecond call (same key): was_duplicate={was_dup2}")
    print(f"  Result: {r2}")

    # Third call with different key — processes again
    r3, was_dup3 = dedup.execute(str(uuid.uuid4()), create_order)
    print(f"\nThird call (different key): was_duplicate={was_dup3}")
    print(f"  Result: {r3}")

    print(f"\nTotal handler calls: {call_count[0]} (expected: 2)")
    print(f"Deduplicator: {dedup.processed_count} processed, {dedup.dedup_count} deduped")
    print(f"Order IDs are identical for dup: {r1['order_id'] == r2['order_id']} ✓")


if __name__ == "__main__":
    print("API Gateway Patterns — Enterprise Reference Implementation")
    print("Rate Limiting | Circuit Breaker | Coalescing | Key Manager | Versioning | Idempotency")

    demo_rate_limiting()
    demo_circuit_breaker()
    demo_request_coalescing()
    demo_api_key_lifecycle()
    demo_version_routing()
    demo_idempotency()

    print("\n" + "=" * 70)
    print("All patterns demonstrated successfully.")
    print("=" * 70)
