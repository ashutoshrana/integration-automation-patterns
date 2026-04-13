"""
42_mcp_security_patterns.py — Model Context Protocol (MCP) Security Validation Patterns

Pure-Python implementation of five MCP security primitives using only the standard
library (``dataclasses``, ``hashlib``, ``time``, ``typing``).  No external dependencies
are required.

    Pattern 1 — MCPToolDefinition:
                Dataclass representing a single MCP tool definition — the
                metadata that a MCP server advertises about a callable tool.
                Fields: ``name`` (tool identifier), ``description`` (human/LLM-
                readable purpose), ``parameters`` (JSON-schema-style dict),
                ``permissions`` (list of required permission strings),
                ``source`` (server URL or package identifier), ``checksum``
                (optional pre-computed SHA-256 hex string, defaults to ``""``).
                :meth:`compute_checksum` computes a deterministic SHA-256 of
                the tool's identity — ``name + description + str(sorted(
                parameters.items()))`` — and returns the hex digest.  The
                checksum enables out-of-band verification: the server signs
                the manifest; the client re-computes and compares.
                :meth:`has_dangerous_permissions` returns ``True`` when the
                tool's ``permissions`` list contains any entry from
                :data:`DANGEROUS_PERMISSIONS` (``delete``, ``external_call``,
                ``write_all``, ``admin``) — providing a fast triage signal
                before deeper validation.

    Pattern 2 — MCPSecurityValidator:
                Validates a :class:`MCPToolDefinition` against four independent
                security policies before the tool is admitted to the registry.
                Constructed with an optional ``trusted_sources`` list; when
                ``None``, all sources are trusted (permissive mode, useful in
                development).  :meth:`validate_source` verifies the tool's
                ``source`` is in the trusted list.  :meth:`validate_metadata`
                scans ``name`` and ``description`` for shell metacharacters
                and code-injection patterns defined in
                :data:`INJECTION_PATTERNS`; a match indicates a compromised or
                malicious server sending a prompt-injection payload.
                :meth:`validate_permissions` ensures the tool's permissions are
                a subset of :data:`ALLOWED_PERMISSIONS`.
                :meth:`validate_checksum` re-computes the tool's checksum and
                compares it to the provided *expected* string; an empty
                *expected* skips the check (opt-in verification).
                :meth:`validate_all` runs all four checks and returns a list
                of ``(ok: bool, reason: str)`` tuples in the order
                source → metadata → permissions → checksum.

    Pattern 3 — MCPToolRegistry:
                Manages the set of approved, registered tools available to an
                agent runtime.  Constructed with a :class:`MCPSecurityValidator`.
                :meth:`register` runs ``validator.validate_all()`` on the
                candidate tool with an optional *expected_checksum*; the tool
                is stored only if **all** validations pass — any single failure
                blocks registration.  Returns ``True`` on success, ``False``
                on rejection.  :meth:`get_tool` returns the registered
                :class:`MCPToolDefinition` by name or ``None``.
                :meth:`list_tools` returns a sorted list of registered tool
                names.  :meth:`is_registered` is a predicate for fast
                membership testing.  :meth:`unregister` removes a tool by name
                and returns ``True`` if it was present, ``False`` otherwise.

    Pattern 4 — MCPInvocationGuard:
                Runtime guard that validates each tool invocation *before* the
                tool is dispatched and records every invocation in an
                append-only audit log.  Constructed with a
                :class:`MCPToolRegistry`.  :meth:`validate_invocation` checks
                three conditions: (1) the tool is registered, (2) no argument
                value contains patterns from :data:`INJECTION_PATTERNS`, and
                (3) no argument key is absent from the tool's declared
                ``parameters`` schema.  Returns ``(True, "")`` on success or
                ``(False, reason)`` on the first failure.
                :meth:`record_invocation` appends a dict record with
                ``tool_name``, ``arguments``, ``user_id``, and ``timestamp``
                to the internal log.  :meth:`get_audit_log` returns the full
                list of records (read-only copy).  :meth:`get_invocation_count`
                returns the number of times a specific tool has been recorded.

    Pattern 5 — MCPRateLimiter:
                Per-tool sliding-window rate limiter.  Constructed with a
                ``default_limit`` (calls per window) and ``window_seconds``
                (window duration in seconds, float for sub-second precision).
                :meth:`set_limit` overrides the default limit for a specific
                tool.  :meth:`check_allowed` evicts expired timestamps from
                the tool's window and returns ``True`` when the count of
                remaining timestamps is below the limit — it does **not**
                record a new call.  :meth:`record_call` appends the current
                monotonic timestamp to the tool's window.  :meth:`remaining`
                returns ``max(0, limit - current_window_count)`` — the number
                of additional calls permitted right now.

Usage example
-------------
::

    # Define and validate a tool
    tool = MCPToolDefinition(
        name="query_records",
        description="Query CRM records by filter field",
        parameters={"table": "str", "filter_value": "str"},
        permissions=["read", "list"],
        source="https://crm-mcp.internal",
    )
    tool.checksum = tool.compute_checksum()

    validator = MCPSecurityValidator(trusted_sources=["https://crm-mcp.internal"])
    registry = MCPToolRegistry(validator)
    registry.register(tool, expected_checksum=tool.checksum)

    guard = MCPInvocationGuard(registry)
    ok, reason = guard.validate_invocation(
        "query_records", {"table": "customers", "filter_value": "acme"}
    )
    assert ok
    guard.record_invocation("query_records", {"table": "customers"}, user_id="agent-1")

    limiter = MCPRateLimiter(default_limit=10, window_seconds=60.0)
    assert limiter.check_allowed("query_records")
    limiter.record_call("query_records")
    assert limiter.remaining("query_records") == 9

No external dependencies required.

Run:
    python examples/42_mcp_security_patterns.py
"""

from __future__ import annotations

import hashlib
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_PERMISSIONS: frozenset[str] = frozenset({"read", "list", "write", "delete", "external_call", "compute"})

DANGEROUS_PERMISSIONS: frozenset[str] = frozenset({"delete", "external_call", "write_all", "admin", "root", "sudo"})

# Patterns that indicate command injection or code injection in tool metadata.
# A tool name or description containing any of these strings is considered
# compromised and must be rejected before registration.
INJECTION_PATTERNS: list[str] = [
    ";",
    "|",
    "&",
    "$",
    "`",
    "$(",
    "${",
    "__import__",
    "eval(",
    "exec(",
]


# ===========================================================================
# Pattern 1 — MCPToolDefinition
# ===========================================================================


@dataclass
class MCPToolDefinition:
    """Represents a single MCP tool definition as advertised by an MCP server.

    Example::

        tool = MCPToolDefinition(
            name="read_customer",
            description="Read a single customer record by ID",
            parameters={"customer_id": "str"},
            permissions=["read"],
            source="https://crm-mcp.internal",
        )
        tool.checksum = tool.compute_checksum()
        assert tool.has_dangerous_permissions() is False
    """

    name: str
    description: str
    parameters: dict[str, Any]
    permissions: list[str]
    source: str
    checksum: str = ""

    def compute_checksum(self) -> str:
        """Compute a deterministic SHA-256 checksum of this tool's identity.

        The checksum covers ``name``, ``description``, and
        ``sorted(parameters.items())`` — the fields that define what the tool
        does.  ``source`` and ``checksum`` are intentionally excluded so that
        the same tool served from different mirrors produces the same hash.

        Returns:
            Lowercase hex SHA-256 digest string (64 characters).
        """
        payload = self.name + self.description + str(sorted(self.parameters.items()))
        return hashlib.sha256(payload.encode()).hexdigest()

    def has_dangerous_permissions(self) -> bool:
        """Return ``True`` when the tool requests at least one dangerous permission.

        Dangerous permissions are those in :data:`DANGEROUS_PERMISSIONS`:
        ``delete``, ``external_call``, ``write_all``, ``admin``, ``root``,
        ``sudo``.  A ``True`` result does not block registration on its own —
        it is a triage signal for downstream policy decisions.

        Returns:
            ``True`` if any permission in ``self.permissions`` is in
            :data:`DANGEROUS_PERMISSIONS`, ``False`` otherwise.
        """
        return bool(set(self.permissions) & DANGEROUS_PERMISSIONS)


# ===========================================================================
# Pattern 2 — MCPSecurityValidator
# ===========================================================================


class MCPSecurityValidator:
    """Validates :class:`MCPToolDefinition` objects against four security policies.

    Policies are evaluated independently so callers can inspect each result.
    All four must pass for a tool to be suitable for registration.

    Example::

        validator = MCPSecurityValidator(
            trusted_sources=["https://crm-mcp.internal"]
        )
        ok, reason = validator.validate_source(tool)
        assert ok
    """

    def __init__(self, trusted_sources: list[str] | None = None) -> None:
        """Initialise the validator.

        Args:
            trusted_sources: Allowlist of source identifiers (URLs, package
                names, etc.).  When ``None``, source validation is skipped and
                every source is considered trusted — useful for development
                environments where the tool origin is always the local process.
        """
        self._trusted_sources: list[str] | None = trusted_sources

    # ------------------------------------------------------------------
    # Individual validators
    # ------------------------------------------------------------------

    def validate_source(self, tool: MCPToolDefinition) -> tuple[bool, str]:
        """Check that the tool's source is in the trusted-sources allowlist.

        Args:
            tool: The tool definition to validate.

        Returns:
            ``(True, "")`` when the source is trusted or no allowlist is
            configured.  ``(False, reason)`` when the source is not on the
            allowlist.
        """
        if self._trusted_sources is None:
            return True, ""
        if tool.source in self._trusted_sources:
            return True, ""
        return False, f"untrusted source: {tool.source!r}"

    def validate_metadata(self, tool: MCPToolDefinition) -> tuple[bool, str]:
        """Scan name and description for shell metacharacters and injection patterns.

        Any match against :data:`INJECTION_PATTERNS` indicates that the MCP
        server is embedding malicious instructions in the tool manifest — a
        known attack vector (CVE-2025-6514 class).

        Args:
            tool: The tool definition to validate.

        Returns:
            ``(True, "")`` when no injection patterns are found.
            ``(False, reason)`` identifying the first matched pattern and
            which field it was found in.
        """
        for field_name, value in (("name", tool.name), ("description", tool.description)):
            for pattern in INJECTION_PATTERNS:
                if pattern in value:
                    return False, f"injection pattern {pattern!r} found in {field_name}"
        return True, ""

    def validate_permissions(self, tool: MCPToolDefinition) -> tuple[bool, str]:
        """Verify that all requested permissions are in :data:`ALLOWED_PERMISSIONS`.

        Args:
            tool: The tool definition to validate.

        Returns:
            ``(True, "")`` when every permission is allowed.
            ``(False, reason)`` listing the first unknown permission.
        """
        unknown = set(tool.permissions) - ALLOWED_PERMISSIONS
        if unknown:
            return False, f"unknown permissions: {sorted(unknown)}"
        return True, ""

    def validate_checksum(self, tool: MCPToolDefinition, expected: str) -> tuple[bool, str]:
        """Verify the tool's checksum against a pre-computed expected value.

        An empty *expected* string skips verification (opt-in mode) and always
        returns ``(True, "")``.  When *expected* is non-empty, the tool's
        checksum is recomputed and compared; a mismatch indicates that the
        manifest was tampered with in transit.

        Args:
            tool: The tool definition to validate.
            expected: Pre-computed expected checksum string.  Pass ``""`` to
                skip verification.

        Returns:
            ``(True, "")`` when *expected* is empty or the recomputed
            checksum matches.  ``(False, reason)`` on mismatch.
        """
        if not expected:
            return True, ""
        actual = tool.compute_checksum()
        if actual == expected:
            return True, ""
        return False, f"checksum mismatch: expected {expected!r}, got {actual!r}"

    # ------------------------------------------------------------------
    # Aggregate validator
    # ------------------------------------------------------------------

    def validate_all(
        self,
        tool: MCPToolDefinition,
        expected_checksum: str = "",
    ) -> list[tuple[bool, str]]:
        """Run all four validations and return results in order.

        Order: source → metadata → permissions → checksum.

        Args:
            tool: The tool definition to validate.
            expected_checksum: Forwarded to :meth:`validate_checksum`.

        Returns:
            A list of four ``(ok, reason)`` tuples in the order described
            above.  Inspect each to understand which policy failed.
        """
        return [
            self.validate_source(tool),
            self.validate_metadata(tool),
            self.validate_permissions(tool),
            self.validate_checksum(tool, expected_checksum),
        ]


# ===========================================================================
# Pattern 3 — MCPToolRegistry
# ===========================================================================


class MCPToolRegistry:
    """Manages the set of security-approved tools available to an agent runtime.

    Only tools that pass **all** validations are stored.  This is an allowlist-
    only registry — unknown tools are rejected by default.

    Example::

        registry = MCPToolRegistry(validator)
        registered = registry.register(tool)
        assert registered
        assert registry.is_registered("read_customer")
        registry.unregister("read_customer")
        assert not registry.is_registered("read_customer")
    """

    def __init__(self, validator: MCPSecurityValidator) -> None:
        """Initialise the registry.

        Args:
            validator: The :class:`MCPSecurityValidator` used to vet every
                candidate tool before admission.
        """
        self._validator = validator
        self._tools: dict[str, MCPToolDefinition] = {}

    def register(self, tool: MCPToolDefinition, expected_checksum: str = "") -> bool:
        """Validate and register a tool definition.

        The tool is stored only when **all** four validation checks pass.

        Args:
            tool: The :class:`MCPToolDefinition` to register.
            expected_checksum: Optional expected checksum forwarded to
                :meth:`MCPSecurityValidator.validate_all`.  Pass ``""`` to
                skip checksum verification.

        Returns:
            ``True`` when the tool was successfully validated and registered.
            ``False`` when any validation failed (tool is not stored).
        """
        results = self._validator.validate_all(tool, expected_checksum)
        if all(ok for ok, _ in results):
            self._tools[tool.name] = tool
            return True
        return False

    def get_tool(self, name: str) -> MCPToolDefinition | None:
        """Return the registered tool by *name*, or ``None`` if not found.

        Args:
            name: The tool name as registered.

        Returns:
            The :class:`MCPToolDefinition` if present, ``None`` otherwise.
        """
        return self._tools.get(name)

    def list_tools(self) -> list[str]:
        """Return a sorted list of all registered tool names.

        Returns:
            Sorted list of tool name strings.
        """
        return sorted(self._tools.keys())

    def is_registered(self, name: str) -> bool:
        """Return ``True`` when a tool with *name* is in the registry.

        Args:
            name: Tool name to check.

        Returns:
            ``True`` if registered, ``False`` otherwise.
        """
        return name in self._tools

    def unregister(self, name: str) -> bool:
        """Remove a tool from the registry by *name*.

        Args:
            name: Tool name to remove.

        Returns:
            ``True`` if the tool was present and removed.
            ``False`` if no tool with that name existed.
        """
        if name in self._tools:
            del self._tools[name]
            return True
        return False


# ===========================================================================
# Pattern 4 — MCPInvocationGuard
# ===========================================================================


class MCPInvocationGuard:
    """Validates tool invocations at runtime and maintains an append-only audit log.

    The guard is the last line of defence before a tool call is dispatched to
    the MCP server.  It verifies that the tool exists, that the arguments are
    free of injection patterns, and that no unexpected argument keys were
    supplied.

    Example::

        guard = MCPInvocationGuard(registry)
        ok, reason = guard.validate_invocation(
            "read_customer", {"customer_id": "cust-123"}
        )
        assert ok
        guard.record_invocation("read_customer", {"customer_id": "cust-123"}, "agent-1")
        assert guard.get_invocation_count("read_customer") == 1
    """

    def __init__(self, registry: MCPToolRegistry) -> None:
        """Initialise the guard.

        Args:
            registry: The :class:`MCPToolRegistry` to check tool membership
                against.
        """
        self._registry = registry
        self._audit_log: list[dict[str, Any]] = []

    def validate_invocation(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> tuple[bool, str]:
        """Validate a proposed tool invocation before dispatch.

        Checks performed in order:
        1. Tool is registered in the registry.
        2. No argument value contains patterns from :data:`INJECTION_PATTERNS`.
        3. No argument key is absent from the tool's declared ``parameters``
           schema (extra keys are rejected — unknown arguments may be attempts
           to exploit undocumented tool behaviours).

        Args:
            tool_name: The name of the tool to invoke.
            arguments: Key-value arguments for the invocation.

        Returns:
            ``(True, "")`` when all checks pass.  ``(False, reason)``
            identifying the first failed check.
        """
        # Check 1: tool must be registered
        tool = self._registry.get_tool(tool_name)
        if tool is None:
            return False, f"tool not registered: {tool_name!r}"

        # Check 2: argument values must not contain injection patterns
        for arg_key, arg_val in arguments.items():
            val_str = str(arg_val)
            for pattern in INJECTION_PATTERNS:
                if pattern in val_str:
                    return False, f"injection pattern {pattern!r} in argument {arg_key!r}"

        # Check 3: no extra argument keys beyond the declared schema
        if tool.parameters:
            extra_keys = set(arguments.keys()) - set(tool.parameters.keys())
            if extra_keys:
                return False, f"unexpected argument keys: {sorted(extra_keys)}"

        return True, ""

    def record_invocation(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        user_id: str,
    ) -> None:
        """Append an invocation record to the audit log.

        The log is append-only — records are never modified or removed.

        Args:
            tool_name: Name of the invoked tool.
            arguments: Arguments passed to the tool (snapshot at call time).
            user_id: Identifier of the requesting agent or user.
        """
        self._audit_log.append(
            {
                "tool_name": tool_name,
                "arguments": dict(arguments),
                "user_id": user_id,
                "timestamp": time.time(),
            }
        )

    def get_audit_log(self) -> list[dict[str, Any]]:
        """Return a copy of the full audit log.

        Returns:
            List of invocation record dicts, in chronological order.
        """
        return list(self._audit_log)

    def get_invocation_count(self, tool_name: str) -> int:
        """Return the number of times *tool_name* appears in the audit log.

        Args:
            tool_name: Tool name to count.

        Returns:
            Integer count (0 if never recorded).
        """
        return sum(1 for record in self._audit_log if record["tool_name"] == tool_name)


# ===========================================================================
# Pattern 5 — MCPRateLimiter
# ===========================================================================


class MCPRateLimiter:
    """Per-tool sliding-window rate limiter for MCP tool invocations.

    Prevents runaway agents from invoking a single tool at unbounded frequency.
    Each tool maintains its own sliding window of call timestamps; window size
    and per-tool limits are configurable.

    Example::

        limiter = MCPRateLimiter(default_limit=5, window_seconds=1.0)
        for _ in range(5):
            assert limiter.check_allowed("my_tool")
            limiter.record_call("my_tool")
        assert not limiter.check_allowed("my_tool")  # limit reached
        assert limiter.remaining("my_tool") == 0
    """

    def __init__(
        self,
        default_limit: int = 100,
        window_seconds: float = 60.0,
    ) -> None:
        """Initialise the rate limiter.

        Args:
            default_limit: Maximum calls per window for tools without a custom
                limit.
            window_seconds: Width of the sliding window in seconds.  Accepts
                fractional values for sub-second windows.
        """
        self._default_limit = default_limit
        self._window_seconds = window_seconds
        # tool_name → deque of monotonic timestamps
        self._windows: dict[str, deque[float]] = {}
        # tool_name → custom limit (overrides default)
        self._limits: dict[str, int] = {}

    def set_limit(self, tool_name: str, limit: int) -> None:
        """Set a custom call limit for *tool_name*.

        Args:
            tool_name: The tool to configure.
            limit: Maximum calls allowed within one window for this tool.
        """
        self._limits[tool_name] = limit

    def _evict(self, tool_name: str) -> None:
        """Remove timestamps older than ``window_seconds`` from the tool's deque."""
        now = time.monotonic()
        cutoff = now - self._window_seconds
        window = self._windows.get(tool_name)
        if window is None:
            return
        while window and window[0] <= cutoff:
            window.popleft()

    def _get_window(self, tool_name: str) -> deque[float]:
        """Return (creating if absent) the timestamp deque for *tool_name*."""
        if tool_name not in self._windows:
            self._windows[tool_name] = deque()
        return self._windows[tool_name]

    def _limit_for(self, tool_name: str) -> int:
        """Return the effective limit for *tool_name*."""
        return self._limits.get(tool_name, self._default_limit)

    def check_allowed(self, tool_name: str) -> bool:
        """Return ``True`` when the tool has remaining calls in the current window.

        Evicts expired timestamps before checking.  Does **not** record a new
        call — call :meth:`record_call` separately to consume a slot.

        Args:
            tool_name: The tool to check.

        Returns:
            ``True`` if the current window count is below the limit.
        """
        self._evict(tool_name)
        window = self._get_window(tool_name)
        return len(window) < self._limit_for(tool_name)

    def record_call(self, tool_name: str) -> None:
        """Record a call to *tool_name* at the current monotonic time.

        Args:
            tool_name: The tool that was called.
        """
        window = self._get_window(tool_name)
        window.append(time.monotonic())

    def remaining(self, tool_name: str) -> int:
        """Return the number of calls remaining in the current window.

        Evicts expired timestamps before computing.

        Args:
            tool_name: The tool to query.

        Returns:
            ``max(0, limit - current_window_count)``.
        """
        self._evict(tool_name)
        window = self._get_window(tool_name)
        limit = self._limit_for(tool_name)
        return max(0, limit - len(window))


# ===========================================================================
# Self-test / usage demo
# ===========================================================================

if __name__ == "__main__":
    # --- MCPToolDefinition ---
    tool = MCPToolDefinition(
        name="read_customer",
        description="Read a single customer record by customer ID",
        parameters={"customer_id": "str"},
        permissions=["read"],
        source="https://crm-mcp.internal",
    )
    checksum = tool.compute_checksum()
    tool.checksum = checksum
    assert not tool.has_dangerous_permissions()
    print(f"Tool checksum: {checksum[:16]}...")

    # --- MCPSecurityValidator ---
    validator = MCPSecurityValidator(trusted_sources=["https://crm-mcp.internal"])
    results = validator.validate_all(tool, expected_checksum=checksum)
    assert all(ok for ok, _ in results), results
    print("All validations passed.")

    # --- MCPToolRegistry ---
    registry = MCPToolRegistry(validator)
    assert registry.register(tool, expected_checksum=checksum)
    assert registry.is_registered("read_customer")
    assert registry.list_tools() == ["read_customer"]

    # Reject a malicious tool with injection in description
    bad_tool = MCPToolDefinition(
        name="bad_tool",
        description="Normal description; rm -rf /",
        parameters={},
        permissions=["read"],
        source="https://crm-mcp.internal",
    )
    assert not registry.register(bad_tool)
    print("Malicious tool correctly rejected.")

    # --- MCPInvocationGuard ---
    guard = MCPInvocationGuard(registry)
    ok, reason = guard.validate_invocation("read_customer", {"customer_id": "cust-001"})
    assert ok, reason
    guard.record_invocation("read_customer", {"customer_id": "cust-001"}, user_id="agent-1")
    assert guard.get_invocation_count("read_customer") == 1
    print(f"Audit log entries: {len(guard.get_audit_log())}")

    # --- MCPRateLimiter ---
    limiter = MCPRateLimiter(default_limit=3, window_seconds=60.0)
    for _ in range(3):
        assert limiter.check_allowed("read_customer")
        limiter.record_call("read_customer")
    assert not limiter.check_allowed("read_customer")
    assert limiter.remaining("read_customer") == 0
    print("Rate limiter correctly enforces limit of 3.")

    print("\nAll self-tests passed.")
