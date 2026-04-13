"""
21_api_gateway_patterns.py — API Gateway patterns for request/response
transformation, rate limiting, API versioning, and backend composition.

Demonstrates five complementary patterns that together implement a production-
grade API gateway layer suitable for routing, protecting, and aggregating
microservice calls:

    Pattern 1 — Rate Limiting (Token Bucket):
                Each client_id is assigned an independent token bucket that
                refills at a configured rate (requests_per_second) up to a
                maximum of burst_size tokens. A request consumes one token;
                when the bucket is empty RateLimitExceeded is raised with the
                computed retry_after_seconds so the caller can implement
                Retry-After header semantics. This is the algorithm used by
                Kong, AWS API Gateway throttling, and Nginx's limit_req_zone
                directive. Unlike the leaky-bucket algorithm (which smooths
                bursts), the token bucket permits short bursts up to burst_size
                while enforcing a long-run average rate, matching real-world
                gateway behaviour.

    Pattern 2 — Request Transformation:
                Declarative rules mutate an incoming APIRequest before it
                reaches a backend handler. Supported operations are header
                injection (add/override a header on every request),  body
                field renaming (move a field from one key to another), and
                URL path rewriting. This mirrors the transformation plugins of
                Kong (request-transformer), AWS API Gateway mapping templates
                (application/json), and MuleSoft's DataWeave transform
                component — all of which decouple consumer contracts from
                backend contracts without touching backend code.

    Pattern 3 — Response Transformation:
                Declarative rules mutate an APIResponse returned by a backend.
                Supported operations are status-code remapping (e.g. translate
                a backend 404 to a 200 with an empty body) and body field
                masking (replace a sensitive field value with "***"). This is
                the counterpart to request transformation and mirrors Kong's
                response-transformer plugin and AWS API Gateway integration
                response mapping templates.

    Pattern 4 — API Version Router:
                Routes an APIRequest to the correct versioned handler based on
                the request's version field ("v1", "v2", "v3"). Unrecognised
                versions raise UnsupportedVersion, giving callers an actionable
                error that includes the list of supported versions. This pattern
                implements the URI-path versioning strategy described in
                Microsoft REST API Guidelines and mirrors the stage routing of
                AWS API Gateway (/{proxy+} with stage variables) and Kong's
                route matching by path prefix.

    Pattern 5 — API Composer (fan-out / scatter-gather):
                Dispatches a single logical request to multiple named backend
                handlers, merges their response bodies (prefixing each backend
                name to avoid key collisions), accumulates backend_calls, and
                averages latency_ms. compose() calls backends sequentially;
                compose_parallel() calls them concurrently via threading.Thread,
                reducing wall-clock latency to the slowest backend rather than
                the sum of all backends. This is the scatter-gather pattern
                used by GraphQL resolvers, Netflix Zuul aggregation filters,
                and BFF (Backend-For-Frontend) aggregation services.

Relationship to commercial API gateway products:
    - Kong (OSS/Enterprise) : RateLimiter ≈ Rate Limiting plugin;
                              RequestTransformer ≈ Request Transformer Advanced;
                              ResponseTransformer ≈ Response Transformer;
                              APIVersionRouter ≈ Route objects + path matching;
                              APIComposer ≈ custom Lua/Go plugin aggregation.
    - AWS API Gateway       : RateLimiter ≈ usage plan throttle (burst + rate);
                              Request/ResponseTransformer ≈ mapping templates;
                              APIVersionRouter ≈ stage variables + custom
                              authorizers; APIComposer ≈ Step Functions
                              integration or Lambda aggregator.
    - MuleSoft Anypoint     : RequestTransformer ≈ DataWeave Transform Message;
                              RateLimiter ≈ Rate Limiting SLA policy;
                              APIComposer ≈ Scatter-Gather flow component.
    - Apigee (Google Cloud) : RateLimiter ≈ SpikeArrest + Quota policies;
                              RequestTransformer ≈ AssignMessage policy;
                              APIVersionRouter ≈ conditional flows by proxy.pathsuffix.
    - Nginx / Envoy         : RateLimiter ≈ limit_req / ratelimit service;
                              RequestTransformer ≈ proxy_set_header / Lua filter;
                              APIVersionRouter ≈ location blocks / virtual hosts.

No external dependencies required.

Run:
    python examples/21_api_gateway_patterns.py
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    """Raised when a client's token bucket is empty and the request cannot proceed.

    Attributes:
        client_id: The identifier of the rate-limited client.
        retry_after_seconds: Minimum seconds the caller should wait before retrying.
    """

    def __init__(self, client_id: str, retry_after_seconds: float) -> None:
        self.client_id = client_id
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Rate limit exceeded for {client_id}; retry after {retry_after_seconds:.2f}s"
        )


class UnsupportedVersion(Exception):
    """Raised when a request targets an API version that has no registered handler.

    Attributes:
        version: The unrecognised version string from the request.
        supported: List of version strings that are currently registered.
    """

    def __init__(self, version: str, supported: list[str]) -> None:
        self.version = version
        self.supported = supported
        super().__init__(
            f"Unsupported API version {version!r}; supported: {supported}"
        )


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class APIRequest:
    """Represents an inbound API request as it enters the gateway.

    Attributes:
        client_id: Opaque identifier used for per-client rate limiting.
        method: HTTP method (GET, POST, PUT, DELETE, PATCH).
        path: Request path, e.g. "/api/v1/users".
        version: Explicit API version string ("v1", "v2", "v3").
        headers: Mutable mapping of HTTP header names to values.
        body: Mutable mapping representing the parsed request body.
        timestamp: Unix epoch float (time.time()) at request receipt.
    """

    client_id: str
    method: str
    path: str
    version: str
    headers: dict[str, str]
    body: dict[str, Any]
    timestamp: float


@dataclass
class APIResponse:
    """Represents a response returned by a backend handler or the gateway.

    Attributes:
        status_code: HTTP status code.
        headers: Mutable mapping of HTTP response header names to values.
        body: Mutable mapping representing the parsed response body.
        latency_ms: Wall-clock milliseconds for the backend call(s).
        backend_calls: Number of downstream backend calls made to produce this response.
    """

    status_code: int
    headers: dict[str, str]
    body: dict[str, Any]
    latency_ms: float
    backend_calls: int = 0


@dataclass
class RateLimitConfig:
    """Configuration parameters for a token-bucket rate limiter.

    Attributes:
        requests_per_second: Sustained refill rate (tokens added per second).
        burst_size: Maximum number of tokens the bucket can hold; governs
                    the maximum burst of requests allowed before throttling.
    """

    requests_per_second: float
    burst_size: int


@dataclass
class TransformRule:
    """Describes a single transformation to apply to a request or response field.

    Attributes:
        field_path: Dot-notation path to the target field, e.g. "user.email".
                    Only top-level fields are supported in this implementation;
                    dot notation is reserved for future nested-field support.
        action: One of "mask", "rename", "inject", or "remove".
        target: For "rename": the new field name.
                For "inject": the value to inject.
                Unused for "mask" and "remove".
    """

    field_path: str
    action: str
    target: str = ""


# ---------------------------------------------------------------------------
# Pattern 1 — RateLimiter (Token Bucket)
# ---------------------------------------------------------------------------


class RateLimiter:
    """Per-client token-bucket rate limiter.

    Each unique client_id receives its own independent bucket initialised to
    burst_size tokens. Tokens refill continuously at requests_per_second based
    on wall-clock elapsed time (lazy refill — computed on each consume call).

    Thread safety: a reentrant lock protects bucket state so that
    compose_parallel can share a single RateLimiter instance safely.

    Example::

        cfg = RateLimitConfig(requests_per_second=5.0, burst_size=10)
        limiter = RateLimiter(cfg)
        limiter.consume("user-abc")     # OK — token consumed
        print(limiter.get_token_count("user-abc"))  # ≈ 9.0
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        # Per-client state: (current_tokens, last_refill_timestamp)
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_bucket(self, client_id: str) -> tuple[float, float]:
        """Return the (tokens, last_refill_time) tuple for *client_id*.

        Creates a full bucket if the client has not been seen before.
        """
        if client_id not in self._buckets:
            self._buckets[client_id] = (float(self._config.burst_size), time.time())
        return self._buckets[client_id]

    def _refill(self, tokens: float, last_refill: float) -> tuple[float, float]:
        """Compute the refilled token count based on elapsed time.

        Returns:
            (new_token_count, new_last_refill_timestamp)
        """
        now = time.time()
        elapsed = now - last_refill
        added = elapsed * self._config.requests_per_second
        new_tokens = min(tokens + added, float(self._config.burst_size))
        return new_tokens, now

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume(self, client_id: str) -> None:
        """Consume one token from *client_id*'s bucket.

        Args:
            client_id: Identifier of the requesting client.

        Raises:
            RateLimitExceeded: If the bucket is empty; includes retry_after_seconds
                               indicating how long to wait for the next token.
        """
        with self._lock:
            tokens, last_refill = self._get_or_create_bucket(client_id)
            tokens, last_refill = self._refill(tokens, last_refill)

            if tokens < 1.0:
                # Compute how many seconds until one token refills.
                deficit = 1.0 - tokens
                retry_after = deficit / self._config.requests_per_second
                self._buckets[client_id] = (tokens, last_refill)
                raise RateLimitExceeded(client_id, retry_after)

            self._buckets[client_id] = (tokens - 1.0, last_refill)

    def get_token_count(self, client_id: str) -> float:
        """Return the current (refilled) token count for *client_id*.

        Does NOT consume a token. Useful for monitoring and testing.

        Args:
            client_id: Identifier of the client to inspect.

        Returns:
            Current floating-point token count in [0.0, burst_size].
        """
        with self._lock:
            tokens, last_refill = self._get_or_create_bucket(client_id)
            tokens, _ = self._refill(tokens, last_refill)
            return tokens


# ---------------------------------------------------------------------------
# Pattern 2 — RequestTransformer
# ---------------------------------------------------------------------------


class RequestTransformer:
    """Applies declarative transformation rules to inbound API requests.

    Rules are registered via add_header_rule and add_body_mapping, then
    applied in registration order by transform(). The original request object
    is never mutated; transform() returns a new APIRequest with a shallow copy
    of headers and body dicts.

    Supports three transformation types:
        - Header injection: adds or overrides a named header on every request.
        - Body field mapping: renames a top-level body key.
        - URL rewriting: replaces an exact path prefix.

    Example::

        rt = RequestTransformer()
        rt.add_header_rule("X-Correlation-ID", "abc-123")
        rt.add_body_mapping("userId", "user_id")
        new_req = rt.transform(request)
    """

    def __init__(self) -> None:
        self._header_rules: list[tuple[str, str]] = []
        self._body_mappings: list[tuple[str, str]] = []
        self._url_rewrites: list[tuple[str, str]] = []

    def add_header_rule(self, header_name: str, value: str) -> None:
        """Register a rule that injects or overrides *header_name* with *value*.

        Args:
            header_name: HTTP header name (case-sensitive).
            value: Value to set on every transformed request.
        """
        self._header_rules.append((header_name, value))

    def add_body_mapping(self, source_field: str, target_field: str) -> None:
        """Register a rule that renames *source_field* to *target_field* in the body.

        If *source_field* is absent from the request body the rule is silently
        skipped, maintaining idempotency.

        Args:
            source_field: Existing top-level key in the request body.
            target_field: New key name in the transformed body.
        """
        self._body_mappings.append((source_field, target_field))

    def add_url_rewrite(self, from_prefix: str, to_prefix: str) -> None:
        """Register a rule that replaces *from_prefix* at the start of the request path.

        Args:
            from_prefix: Path prefix to match (checked with str.startswith).
            to_prefix: Replacement prefix.
        """
        self._url_rewrites.append((from_prefix, to_prefix))

    def transform(self, request: APIRequest) -> APIRequest:
        """Apply all registered rules to *request* and return a new APIRequest.

        Processing order: header injection → body mappings → URL rewrites.

        Args:
            request: The original inbound APIRequest (not mutated).

        Returns:
            A new APIRequest with transformations applied.
        """
        new_headers = dict(request.headers)
        new_body = dict(request.body)
        new_path = request.path

        # --- Header injection ---
        for header_name, value in self._header_rules:
            new_headers[header_name] = value

        # --- Body field renaming ---
        for source_field, target_field in self._body_mappings:
            if source_field in new_body:
                new_body[target_field] = new_body.pop(source_field)

        # --- URL path rewriting ---
        for from_prefix, to_prefix in self._url_rewrites:
            if new_path.startswith(from_prefix):
                new_path = to_prefix + new_path[len(from_prefix):]
                break  # Apply only the first matching rewrite rule.

        return APIRequest(
            client_id=request.client_id,
            method=request.method,
            path=new_path,
            version=request.version,
            headers=new_headers,
            body=new_body,
            timestamp=request.timestamp,
        )


# ---------------------------------------------------------------------------
# Pattern 3 — ResponseTransformer
# ---------------------------------------------------------------------------


class ResponseTransformer:
    """Applies declarative transformation rules to backend API responses.

    Supports two transformation types:
        - Status code remapping: translates one HTTP status code to another,
          optionally clearing the response body (used to hide backend errors
          from consumers or to normalize diverse backend response codes).
        - Body field masking: replaces the value of a named body field with
          "***" to prevent sensitive data (e.g. passwords, tokens) from
          leaking to consumers.

    Example::

        rt = ResponseTransformer()
        rt.add_status_mapping(404, 200)   # make 404 look like an empty 200
        rt.add_body_mask("password")
        new_resp = rt.transform(response)
    """

    def __init__(self) -> None:
        self._status_mappings: dict[int, int] = {}
        self._masked_fields: list[str] = []

    def add_status_mapping(self, from_code: int, to_code: int) -> None:
        """Register a status code remapping rule.

        When the response's status_code equals *from_code*, it is replaced
        with *to_code*. If *to_code* is 2xx and *from_code* was 4xx/5xx, the
        response body is cleared to an empty dict to avoid leaking error
        details.

        Args:
            from_code: HTTP status code to match.
            to_code: HTTP status code to set in the transformed response.
        """
        self._status_mappings[from_code] = to_code

    def add_body_mask(self, field_path: str) -> None:
        """Register a field masking rule.

        The named top-level field (dot-notation reserved for future expansion)
        will be replaced with "***" if present in the response body.

        Args:
            field_path: Top-level key to mask, e.g. "password" or "ssn".
        """
        self._masked_fields.append(field_path)

    def transform(self, response: APIResponse) -> APIResponse:
        """Apply all registered rules to *response* and return a new APIResponse.

        Processing order: status remapping → body masking.

        Args:
            response: The original backend APIResponse (not mutated).

        Returns:
            A new APIResponse with transformations applied.
        """
        new_status = response.status_code
        new_body = dict(response.body)
        new_headers = dict(response.headers)

        # --- Status code remapping ---
        if new_status in self._status_mappings:
            mapped_status = self._status_mappings[new_status]
            # Clear the body when remapping an error to a 2xx to avoid leaking
            # backend error details.
            if new_status >= 400 and 200 <= mapped_status < 300:
                new_body = {}
            new_status = mapped_status

        # --- Body field masking ---
        for field_path in self._masked_fields:
            if field_path in new_body:
                new_body[field_path] = "***"

        return APIResponse(
            status_code=new_status,
            headers=new_headers,
            body=new_body,
            latency_ms=response.latency_ms,
            backend_calls=response.backend_calls,
        )


# ---------------------------------------------------------------------------
# Pattern 4 — APIVersionRouter
# ---------------------------------------------------------------------------


class APIVersionRouter:
    """Routes inbound requests to version-specific backend handlers.

    Each version string (e.g. "v1", "v2") is mapped to a callable with the
    signature ``(APIRequest) -> APIResponse``. The route() method dispatches
    based on the request's version field and raises UnsupportedVersion for
    unregistered versions, giving callers an actionable error with the list
    of currently supported versions.

    Example::

        router = APIVersionRouter()
        router.register("v1", v1_handler)
        router.register("v2", v2_handler)
        response = router.route(request)   # dispatches by request.version
    """

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[APIRequest], APIResponse]] = {}

    def register(self, version: str, handler: Callable[[APIRequest], APIResponse]) -> None:
        """Associate *version* with *handler*.

        Registering the same version twice silently replaces the previous
        handler, enabling hot-swap of handlers in tests and canary deployments.

        Args:
            version: Version string, e.g. "v1", "v2", "v3".
            handler: Callable that accepts an APIRequest and returns APIResponse.
        """
        self._handlers[version] = handler

    def route(self, request: APIRequest) -> APIResponse:
        """Dispatch *request* to the handler registered for request.version.

        Args:
            request: Inbound APIRequest whose version field selects the handler.

        Returns:
            APIResponse returned by the matched handler.

        Raises:
            UnsupportedVersion: If request.version has no registered handler.
        """
        handler = self._handlers.get(request.version)
        if handler is None:
            raise UnsupportedVersion(request.version, self.supported_versions)
        return handler(request)

    @property
    def supported_versions(self) -> list[str]:
        """Sorted list of version strings that have registered handlers."""
        return sorted(self._handlers.keys())


# ---------------------------------------------------------------------------
# Pattern 5 — APIComposer (scatter-gather)
# ---------------------------------------------------------------------------


class APIComposer:
    """Fan-out gateway that calls multiple backends and merges their responses.

    Each registered backend has a unique name and a handler callable with the
    signature ``(APIRequest) -> APIResponse``. On compose(), every backend is
    called with the same request; their response bodies are merged into a
    single dict with each key prefixed by the backend name and a dot
    (e.g. backend "users" body key "id" becomes "users.id"), their
    backend_calls counts are summed, and latency_ms is averaged.

    compose() is sequential (backends called one after another).
    compose_parallel() calls all backends concurrently using threading.Thread,
    reducing wall-clock latency to the duration of the slowest backend rather
    than the sum.

    Example::

        composer = APIComposer()
        composer.add_backend("profile", profile_handler)
        composer.add_backend("orders", orders_handler)
        response = composer.compose(request)
        # response.body == {"profile.name": "Alice", "orders.count": 3, ...}
    """

    def __init__(self) -> None:
        self._backends: dict[str, Callable[[APIRequest], APIResponse]] = {}

    def add_backend(self, name: str, handler: Callable[[APIRequest], APIResponse]) -> None:
        """Register a named backend handler.

        Args:
            name: Unique backend name used as a prefix in the merged body.
            handler: Callable that accepts an APIRequest and returns APIResponse.
        """
        self._backends[name] = handler

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_responses(
        named_responses: list[tuple[str, APIResponse]],
    ) -> APIResponse:
        """Merge a list of (backend_name, APIResponse) into a single APIResponse.

        Merge rules:
          - body: keys are prefixed as "<backend_name>.<key>"; later backends
            do not overwrite earlier ones when there is a collision because
            the prefix ensures uniqueness.
          - status_code: the lowest (most severe) non-2xx code wins; if all
            are 2xx, 200 is returned.
          - backend_calls: summed.
          - latency_ms: arithmetic mean of all individual backend latencies.
          - headers: merged; later backends' headers override earlier ones.

        Args:
            named_responses: Ordered list of (name, response) pairs.

        Returns:
            A single merged APIResponse.
        """
        merged_body: dict[str, Any] = {}
        total_backend_calls = 0
        total_latency = 0.0
        merged_headers: dict[str, str] = {}
        final_status = 200

        for name, resp in named_responses:
            # Prefix each body key with the backend name.
            for k, v in resp.body.items():
                merged_body[f"{name}.{k}"] = v

            total_backend_calls += resp.backend_calls if resp.backend_calls > 0 else 1
            total_latency += resp.latency_ms
            merged_headers.update(resp.headers)

            # Track the most severe (lowest successful / highest error) status.
            if resp.status_code >= 400 and final_status < 400:
                final_status = resp.status_code
            elif resp.status_code >= 400 and resp.status_code > final_status:
                final_status = resp.status_code

        avg_latency = total_latency / len(named_responses) if named_responses else 0.0

        return APIResponse(
            status_code=final_status,
            headers=merged_headers,
            body=merged_body,
            latency_ms=avg_latency,
            backend_calls=total_backend_calls,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compose(self, request: APIRequest) -> APIResponse:
        """Call all registered backends sequentially and merge their responses.

        Backends are called in insertion order. If no backends are registered
        an empty 200 response is returned.

        Args:
            request: The APIRequest forwarded unchanged to every backend.

        Returns:
            A merged APIResponse combining all backend responses.
        """
        named_responses: list[tuple[str, APIResponse]] = []
        for name, handler in self._backends.items():
            resp = handler(request)
            named_responses.append((name, resp))
        return self._merge_responses(named_responses)

    def compose_parallel(self, request: APIRequest) -> APIResponse:
        """Call all registered backends concurrently and merge their responses.

        Each backend is invoked in its own daemon thread. The method blocks
        until all threads complete, then merges results in insertion order
        (same deterministic merge order as compose()).

        Thread safety: each thread writes to its own result slot; no shared
        mutable state is accessed during backend execution.

        Args:
            request: The APIRequest forwarded unchanged to every backend.

        Returns:
            A merged APIResponse combining all backend responses, with
            latency_ms reflecting the average (not the sum) of per-backend
            wall-clock times.
        """
        names = list(self._backends.keys())
        results: list[Optional[APIResponse]] = [None] * len(names)

        def _call(index: int, name: str, handler: Callable[[APIRequest], APIResponse]) -> None:
            results[index] = handler(request)

        threads = [
            threading.Thread(
                target=_call,
                args=(i, name, self._backends[name]),
                daemon=True,
            )
            for i, name in enumerate(names)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        named_responses: list[tuple[str, APIResponse]] = [
            (names[i], results[i])  # type: ignore[arg-type]
            for i in range(len(names))
            if results[i] is not None
        ]
        return self._merge_responses(named_responses)


# ===========================================================================
# Smoke-test helpers and scenarios (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _make_request(
    client_id: str = "client-1",
    method: str = "GET",
    path: str = "/api/users",
    version: str = "v1",
    headers: Optional[dict[str, str]] = None,
    body: Optional[dict[str, Any]] = None,
) -> APIRequest:
    return APIRequest(
        client_id=client_id,
        method=method,
        path=path,
        version=version,
        headers=headers or {},
        body=body or {},
        timestamp=time.time(),
    )


def _run_rate_limiter_scenario() -> None:
    """Scenario 1: Token bucket allows burst_size requests then throttles."""
    _separator("Scenario 1 — RateLimiter (token bucket)")

    cfg = RateLimitConfig(requests_per_second=5.0, burst_size=3)
    limiter = RateLimiter(cfg)

    # Consume 3 tokens (the full burst).
    for i in range(cfg.burst_size):
        limiter.consume("client-1")
        print(f"  consume #{i + 1} OK; tokens remaining ≈ {limiter.get_token_count('client-1'):.2f}")

    # Next consume should raise.
    try:
        limiter.consume("client-1")
        raise AssertionError("Expected RateLimitExceeded was not raised")
    except RateLimitExceeded as exc:
        print(f"  RateLimitExceeded raised as expected: retry_after={exc.retry_after_seconds:.3f}s")
        assert exc.client_id == "client-1"
        assert exc.retry_after_seconds > 0.0

    # Different clients are independent.
    limiter.consume("client-2")
    print("  client-2 consume OK (independent bucket)")
    print("PASS")


def _run_request_transformer_scenario() -> None:
    """Scenario 2: RequestTransformer injects header, renames body field, rewrites URL."""
    _separator("Scenario 2 — RequestTransformer")

    rt = RequestTransformer()
    rt.add_header_rule("X-Request-ID", "req-999")
    rt.add_header_rule("X-Gateway-Version", "1.0")
    rt.add_body_mapping("userId", "user_id")
    rt.add_url_rewrite("/legacy/", "/api/v2/")

    req = _make_request(
        path="/legacy/orders",
        headers={"Content-Type": "application/json"},
        body={"userId": "usr-42", "amount": 100},
    )
    transformed = rt.transform(req)

    print(f"  Original path     : {req.path!r}")
    print(f"  Transformed path  : {transformed.path!r}")
    print(f"  Original headers  : {req.headers}")
    print(f"  Transformed headers: {transformed.headers}")
    print(f"  Original body     : {req.body}")
    print(f"  Transformed body  : {transformed.body}")

    assert transformed.headers["X-Request-ID"] == "req-999"
    assert transformed.headers["X-Gateway-Version"] == "1.0"
    assert transformed.headers["Content-Type"] == "application/json"  # preserved
    assert "user_id" in transformed.body
    assert "userId" not in transformed.body
    assert transformed.body["user_id"] == "usr-42"
    assert transformed.path == "/api/v2/orders"
    # Original request unchanged.
    assert req.path == "/legacy/orders"
    assert "userId" in req.body
    print("PASS")


def _run_response_transformer_scenario() -> None:
    """Scenario 3: ResponseTransformer remaps 404→200 and masks password field."""
    _separator("Scenario 3 — ResponseTransformer")

    rt = ResponseTransformer()
    rt.add_status_mapping(404, 200)
    rt.add_body_mask("password")

    # 404 response with sensitive body.
    resp = APIResponse(
        status_code=404,
        headers={},
        body={"error": "not found", "password": "secret123"},
        latency_ms=12.5,
    )
    transformed = rt.transform(resp)

    print(f"  Original status   : {resp.status_code}")
    print(f"  Transformed status: {transformed.status_code}")
    print(f"  Original body     : {resp.body}")
    print(f"  Transformed body  : {transformed.body}")

    assert transformed.status_code == 200
    # Body cleared because we remapped a 4xx → 2xx.
    assert transformed.body == {}

    # 200 response with password field — only masking applies.
    resp2 = APIResponse(
        status_code=200,
        headers={},
        body={"username": "alice", "password": "hunter2"},
        latency_ms=8.0,
    )
    transformed2 = rt.transform(resp2)
    assert transformed2.status_code == 200
    assert transformed2.body["password"] == "***"
    assert transformed2.body["username"] == "alice"
    print(f"  Masked body       : {transformed2.body}")
    print("PASS")


def _run_version_router_scenario() -> None:
    """Scenario 4: APIVersionRouter dispatches to correct handler; raises UnsupportedVersion."""
    _separator("Scenario 4 — APIVersionRouter")

    router = APIVersionRouter()

    def v1_handler(r: APIRequest) -> APIResponse:
        return APIResponse(200, {}, {"version": "v1", "legacy": True}, latency_ms=5.0)

    def v2_handler(r: APIRequest) -> APIResponse:
        return APIResponse(200, {}, {"version": "v2", "features": ["pagination"]}, latency_ms=4.0)

    router.register("v1", v1_handler)
    router.register("v2", v2_handler)

    req_v1 = _make_request(version="v1")
    req_v2 = _make_request(version="v2")
    req_v99 = _make_request(version="v99")

    resp_v1 = router.route(req_v1)
    resp_v2 = router.route(req_v2)

    print(f"  v1 response body  : {resp_v1.body}")
    print(f"  v2 response body  : {resp_v2.body}")
    print(f"  Supported versions: {router.supported_versions}")

    assert resp_v1.body["version"] == "v1"
    assert resp_v2.body["version"] == "v2"
    assert router.supported_versions == ["v1", "v2"]

    # Unsupported version raises.
    try:
        router.route(req_v99)
        raise AssertionError("Expected UnsupportedVersion was not raised")
    except UnsupportedVersion as exc:
        print(f"  UnsupportedVersion raised: {exc}")
        assert exc.version == "v99"
        assert "v1" in exc.supported
        assert "v2" in exc.supported
    print("PASS")


def _run_api_composer_scenario() -> None:
    """Scenario 5a: APIComposer sequential fan-out merges body with name prefixes."""
    _separator("Scenario 5a — APIComposer (sequential)")

    def profile_handler(r: APIRequest) -> APIResponse:
        return APIResponse(200, {}, {"name": "Alice", "email": "alice@example.com"}, latency_ms=10.0)

    def orders_handler(r: APIRequest) -> APIResponse:
        return APIResponse(200, {}, {"count": 3, "total": 149.99}, latency_ms=15.0)

    def inventory_handler(r: APIRequest) -> APIResponse:
        return APIResponse(200, {}, {"in_stock": True, "sku": "SKU-007"}, latency_ms=8.0)

    composer = APIComposer()
    composer.add_backend("profile", profile_handler)
    composer.add_backend("orders", orders_handler)
    composer.add_backend("inventory", inventory_handler)

    req = _make_request()
    resp = composer.compose(req)

    print(f"  Merged body       : {resp.body}")
    print(f"  backend_calls     : {resp.backend_calls}")
    print(f"  avg latency_ms    : {resp.latency_ms:.2f}")

    assert "profile.name" in resp.body
    assert resp.body["profile.name"] == "Alice"
    assert "orders.count" in resp.body
    assert resp.body["orders.count"] == 3
    assert "inventory.in_stock" in resp.body
    assert resp.backend_calls == 3
    # Average of 10, 15, 8 = 11.0
    assert abs(resp.latency_ms - 11.0) < 0.01, f"Expected avg 11.0 ms, got {resp.latency_ms}"
    print("PASS")


def _run_api_composer_parallel_scenario() -> None:
    """Scenario 5b: APIComposer parallel fan-out produces same merged body."""
    _separator("Scenario 5b — APIComposer (parallel)")

    def slow_a(r: APIRequest) -> APIResponse:
        time.sleep(0.02)
        return APIResponse(200, {}, {"value": "A"}, latency_ms=20.0)

    def slow_b(r: APIRequest) -> APIResponse:
        time.sleep(0.02)
        return APIResponse(200, {}, {"value": "B"}, latency_ms=20.0)

    composer = APIComposer()
    composer.add_backend("svc_a", slow_a)
    composer.add_backend("svc_b", slow_b)

    req = _make_request()

    t0 = time.time()
    resp = composer.compose_parallel(req)
    wall_clock = time.time() - t0

    print(f"  Merged body       : {resp.body}")
    print(f"  backend_calls     : {resp.backend_calls}")
    print(f"  Wall-clock (s)    : {wall_clock:.3f}  (should be ~0.02, not ~0.04)")

    assert resp.body["svc_a.value"] == "A"
    assert resp.body["svc_b.value"] == "B"
    assert resp.backend_calls == 2
    # Parallel execution: wall clock should be significantly less than 0.04s.
    assert wall_clock < 0.035, f"Parallel compose took too long: {wall_clock:.3f}s"
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("API Gateway Patterns — Smoke Test Suite")
    print("Patterns: RateLimiter | RequestTransformer | ResponseTransformer")
    print("          APIVersionRouter | APIComposer")

    _run_rate_limiter_scenario()
    _run_request_transformer_scenario()
    _run_response_transformer_scenario()
    _run_version_router_scenario()
    _run_api_composer_scenario()
    _run_api_composer_parallel_scenario()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
