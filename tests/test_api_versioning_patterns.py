"""
Tests for 41_api_versioning_patterns.py

Covers all five patterns:
  - SemanticVersion (parse, compare, compatibility, breaking change, str)
  - APIContract (validate_request, validate_response, backward compat)
  - VersionRouter (exact match, compatible fallback, latest, available)
  - ChangelogEntry (fields, is_breaking)
  - APIVersionManager (register, deprecate, changelog filtering, breaking)

Uses only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "examples" / "41_api_versioning_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "api_versioning_patterns_41"
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
# SemanticVersion — parse
# ===========================================================================


class TestSemanticVersionParse:
    def test_parse_basic(self, mod):
        v = mod.SemanticVersion.parse("1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_parse_zeros(self, mod):
        v = mod.SemanticVersion.parse("0.0.0")
        assert v.major == 0
        assert v.minor == 0
        assert v.patch == 0

    def test_parse_large_numbers(self, mod):
        v = mod.SemanticVersion.parse("100.200.300")
        assert v.major == 100
        assert v.minor == 200
        assert v.patch == 300

    def test_parse_strips_whitespace(self, mod):
        v = mod.SemanticVersion.parse("  2.1.0  ")
        assert v.major == 2
        assert v.minor == 1
        assert v.patch == 0

    def test_parse_invalid_missing_patch(self, mod):
        with pytest.raises(ValueError):
            mod.SemanticVersion.parse("1.2")

    def test_parse_invalid_too_many_parts(self, mod):
        with pytest.raises(ValueError):
            mod.SemanticVersion.parse("1.2.3.4")

    def test_parse_invalid_non_integer(self, mod):
        with pytest.raises(ValueError):
            mod.SemanticVersion.parse("1.x.0")

    def test_parse_invalid_empty_string(self, mod):
        with pytest.raises(ValueError):
            mod.SemanticVersion.parse("")

    def test_parse_invalid_negative(self, mod):
        with pytest.raises(ValueError):
            mod.SemanticVersion.parse("-1.0.0")

    def test_parse_non_string_raises(self, mod):
        with pytest.raises((ValueError, AttributeError)):
            mod.SemanticVersion.parse(123)  # type: ignore[arg-type]


# ===========================================================================
# SemanticVersion — str and equality
# ===========================================================================


class TestSemanticVersionStr:
    def test_str_round_trips(self, mod):
        v = mod.SemanticVersion.parse("3.4.5")
        assert str(v) == "3.4.5"

    def test_equality_same_values(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v2 = mod.SemanticVersion.parse("1.0.0")
        assert v1 == v2

    def test_inequality_different_minor(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v2 = mod.SemanticVersion.parse("1.1.0")
        assert v1 != v2

    def test_frozen_immutable(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        with pytest.raises((AttributeError, TypeError)):
            v.major = 99  # type: ignore[misc]


# ===========================================================================
# SemanticVersion — comparison operators
# ===========================================================================


class TestSemanticVersionComparison:
    def test_lt_patch(self, mod):
        assert mod.SemanticVersion.parse("1.0.0") < mod.SemanticVersion.parse("1.0.1")

    def test_lt_minor(self, mod):
        assert mod.SemanticVersion.parse("1.0.9") < mod.SemanticVersion.parse("1.1.0")

    def test_lt_major(self, mod):
        assert mod.SemanticVersion.parse("1.9.9") < mod.SemanticVersion.parse("2.0.0")

    def test_le_equal(self, mod):
        v = mod.SemanticVersion.parse("2.0.0")
        assert v <= v

    def test_le_lesser(self, mod):
        assert mod.SemanticVersion.parse("1.0.0") <= mod.SemanticVersion.parse("1.0.1")

    def test_gt(self, mod):
        assert mod.SemanticVersion.parse("2.0.0") > mod.SemanticVersion.parse("1.9.9")

    def test_ge_equal(self, mod):
        v = mod.SemanticVersion.parse("1.1.1")
        assert v >= v

    def test_ge_greater(self, mod):
        assert mod.SemanticVersion.parse("1.1.0") >= mod.SemanticVersion.parse("1.0.9")

    def test_sorted_order(self, mod):
        versions = [mod.SemanticVersion.parse(s) for s in ["2.0.0", "1.0.0", "1.1.0", "1.0.1"]]
        result = sorted(versions)
        assert result == [
            mod.SemanticVersion.parse("1.0.0"),
            mod.SemanticVersion.parse("1.0.1"),
            mod.SemanticVersion.parse("1.1.0"),
            mod.SemanticVersion.parse("2.0.0"),
        ]

    def test_not_implemented_comparison(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        result = v.__lt__("not_a_version")
        assert result is NotImplemented


# ===========================================================================
# SemanticVersion — compatibility and breaking change
# ===========================================================================


class TestSemanticVersionCompatibility:
    def test_compatible_same_version(self, mod):
        v = mod.SemanticVersion.parse("1.2.3")
        assert v.is_compatible_with(v)

    def test_compatible_higher_minor(self, mod):
        v_new = mod.SemanticVersion.parse("1.3.0")
        v_old = mod.SemanticVersion.parse("1.2.0")
        assert v_new.is_compatible_with(v_old)

    def test_compatible_higher_patch(self, mod):
        v_new = mod.SemanticVersion.parse("1.2.5")
        v_old = mod.SemanticVersion.parse("1.2.3")
        assert v_new.is_compatible_with(v_old)

    def test_incompatible_lower_minor(self, mod):
        v_old = mod.SemanticVersion.parse("1.1.0")
        v_new = mod.SemanticVersion.parse("1.2.0")
        assert not v_old.is_compatible_with(v_new)

    def test_incompatible_different_major(self, mod):
        v1 = mod.SemanticVersion.parse("1.5.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        assert not v1.is_compatible_with(v2)
        assert not v2.is_compatible_with(v1)

    def test_breaking_change_higher_major(self, mod):
        v2 = mod.SemanticVersion.parse("2.0.0")
        v1 = mod.SemanticVersion.parse("1.9.9")
        assert v2.is_breaking_change_from(v1)

    def test_not_breaking_same_major(self, mod):
        v1_1 = mod.SemanticVersion.parse("1.1.0")
        v1_0 = mod.SemanticVersion.parse("1.0.0")
        assert not v1_1.is_breaking_change_from(v1_0)

    def test_not_breaking_same_version(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        assert not v.is_breaking_change_from(v)

    def test_not_breaking_lower_major(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        assert not v1.is_breaking_change_from(v2)


# ===========================================================================
# APIContract — validate_request
# ===========================================================================


class TestAPIContractValidateRequest:
    @pytest.fixture
    def contract(self, mod):
        return mod.APIContract(
            name="orders",
            version=mod.SemanticVersion.parse("1.0.0"),
            required_fields=["order_id", "amount", "customer_id"],
            optional_fields=["note"],
        )

    def test_valid_request_all_required(self, contract):
        valid, missing = contract.validate_request({"order_id": "o1", "amount": 100, "customer_id": "c1"})
        assert valid is True
        assert missing == []

    def test_valid_request_with_optional(self, contract):
        valid, missing = contract.validate_request(
            {"order_id": "o1", "amount": 100, "customer_id": "c1", "note": "urgent"}
        )
        assert valid is True
        assert missing == []

    def test_invalid_missing_one_required(self, contract):
        valid, missing = contract.validate_request({"order_id": "o1", "amount": 100})
        assert valid is False
        assert "customer_id" in missing

    def test_invalid_missing_all_required(self, contract):
        valid, missing = contract.validate_request({})
        assert valid is False
        assert len(missing) == 3

    def test_optional_absence_does_not_fail(self, contract):
        valid, missing = contract.validate_request({"order_id": "o1", "amount": 100, "customer_id": "c1"})
        assert valid is True

    def test_extra_fields_are_allowed(self, contract):
        valid, missing = contract.validate_request({"order_id": "o1", "amount": 100, "customer_id": "c1", "extra": "x"})
        assert valid is True

    def test_no_required_fields_always_valid(self, mod):
        contract = mod.APIContract("ping", mod.SemanticVersion.parse("1.0.0"), [])
        valid, missing = contract.validate_request({})
        assert valid is True
        assert missing == []


# ===========================================================================
# APIContract — validate_response
# ===========================================================================


class TestAPIContractValidateResponse:
    @pytest.fixture
    def contract(self, mod):
        return mod.APIContract("users", mod.SemanticVersion.parse("1.0.0"), ["user_id"])

    def test_valid_response_all_expected(self, contract):
        valid, missing = contract.validate_response(
            {"user_id": "u1", "email": "a@b.com", "status": "active"},
            expected_fields=["user_id", "email"],
        )
        assert valid is True
        assert missing == []

    def test_invalid_response_missing_field(self, contract):
        valid, missing = contract.validate_response(
            {"user_id": "u1"},
            expected_fields=["user_id", "email"],
        )
        assert valid is False
        assert "email" in missing

    def test_empty_expected_always_valid(self, contract):
        valid, missing = contract.validate_response({}, expected_fields=[])
        assert valid is True

    def test_all_fields_missing(self, contract):
        valid, missing = contract.validate_response({}, expected_fields=["a", "b", "c"])
        assert valid is False
        assert set(missing) == {"a", "b", "c"}


# ===========================================================================
# APIContract — backward compatibility
# ===========================================================================


class TestAPIContractBackwardCompat:
    def test_backward_compat_same_required(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v1_1 = mod.SemanticVersion.parse("1.1.0")
        old = mod.APIContract("api", v1, ["id", "name"])
        new = mod.APIContract("api", v1_1, ["id", "name", "email"])
        assert new.is_backward_compatible_with(old)

    def test_backward_compat_identical_contracts(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        c = mod.APIContract("api", v, ["id", "name"])
        assert c.is_backward_compatible_with(c)

    def test_not_backward_compat_removed_field(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        old = mod.APIContract("api", v1, ["id", "name", "email"])
        new = mod.APIContract("api", v2, ["id", "name"])  # email removed
        assert not new.is_backward_compatible_with(old)

    def test_empty_old_required_always_compat(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        old = mod.APIContract("api", v1, [])
        new = mod.APIContract("api", v2, ["id"])
        assert new.is_backward_compatible_with(old)

    def test_stores_optional_fields(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        c = mod.APIContract("api", v, ["id"], optional_fields=["note", "tags"])
        assert "note" in c.optional_fields
        assert "tags" in c.optional_fields

    def test_none_optional_becomes_empty_list(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        c = mod.APIContract("api", v, ["id"], optional_fields=None)
        assert c.optional_fields == []


# ===========================================================================
# VersionRouter
# ===========================================================================


class TestVersionRouterBasic:
    @pytest.fixture
    def router_with_handlers(self, mod):
        router = mod.VersionRouter()
        v1 = mod.SemanticVersion.parse("1.0.0")
        v1_1 = mod.SemanticVersion.parse("1.1.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        router.register(v1, lambda p: {"v": "1.0.0", "payload": p})
        router.register(v1_1, lambda p: {"v": "1.1.0", "payload": p})
        router.register(v2, lambda p: {"v": "2.0.0", "payload": p})
        return router

    def test_exact_match_v1(self, router_with_handlers):
        result = router_with_handlers.route("1.0.0", {})
        assert result["v"] == "1.0.0"

    def test_exact_match_v1_1(self, router_with_handlers):
        result = router_with_handlers.route("1.1.0", {})
        assert result["v"] == "1.1.0"

    def test_exact_match_v2(self, router_with_handlers):
        result = router_with_handlers.route("2.0.0", {})
        assert result["v"] == "2.0.0"

    def test_payload_forwarded(self, router_with_handlers):
        result = router_with_handlers.route("1.0.0", {"key": "value"})
        assert result["payload"] == {"key": "value"}

    def test_compatible_fallback_to_higher_minor(self, mod):
        router = mod.VersionRouter()
        v1_2 = mod.SemanticVersion.parse("1.2.0")
        router.register(v1_2, lambda p: {"v": "1.2.0"})
        # Request 1.0.0 — compatible with 1.2.0 (same major, 1.2 >= 1.0)
        result = router.route("1.0.0", {})
        assert result["v"] == "1.2.0"

    def test_compatible_picks_highest_among_multiple(self, mod):
        router = mod.VersionRouter()
        v1_1 = mod.SemanticVersion.parse("1.1.0")
        v1_3 = mod.SemanticVersion.parse("1.3.0")
        router.register(v1_1, lambda p: {"v": "1.1.0"})
        router.register(v1_3, lambda p: {"v": "1.3.0"})
        # Request 1.0.0 — both 1.1 and 1.3 are compatible; pick highest
        result = router.route("1.0.0", {})
        assert result["v"] == "1.3.0"

    def test_no_compatible_handler_raises(self, router_with_handlers):
        with pytest.raises(ValueError):
            router_with_handlers.route("3.0.0", {})

    def test_different_major_not_compatible(self, mod):
        router = mod.VersionRouter()
        router.register(mod.SemanticVersion.parse("1.0.0"), lambda p: "v1")
        with pytest.raises(ValueError):
            router.route("2.0.0", {})

    def test_invalid_version_string_raises(self, mod):
        router = mod.VersionRouter()
        router.register(mod.SemanticVersion.parse("1.0.0"), lambda p: "v1")
        with pytest.raises(ValueError):
            router.route("bad-version", {})


class TestVersionRouterMetadata:
    @pytest.fixture
    def router(self, mod):
        r = mod.VersionRouter()
        for s in ["1.0.0", "1.2.0", "2.0.0"]:
            r.register(mod.SemanticVersion.parse(s), lambda p, v=s: v)
        return r

    def test_available_versions_sorted(self, router, mod):
        versions = router.available_versions()
        assert versions == sorted(versions)
        assert len(versions) == 3

    def test_latest_version(self, router, mod):
        assert router.latest_version() == mod.SemanticVersion.parse("2.0.0")

    def test_latest_version_empty_raises(self, mod):
        router = mod.VersionRouter()
        with pytest.raises(ValueError):
            router.latest_version()

    def test_available_versions_empty(self, mod):
        router = mod.VersionRouter()
        assert router.available_versions() == []


# ===========================================================================
# ChangelogEntry
# ===========================================================================


class TestChangelogEntry:
    @pytest.fixture
    def entries(self, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        v1_1 = mod.SemanticVersion.parse("1.1.0")
        v2 = mod.SemanticVersion.parse("2.0.0")
        return {
            "feature": mod.ChangelogEntry(v1, "FEATURE", "Initial release", "2024-01-01"),
            "fix": mod.ChangelogEntry(v1_1, "FIX", "Fix null pointer", "2024-02-01"),
            "breaking": mod.ChangelogEntry(v2, "BREAKING", "Remove old endpoint", "2024-06-01"),
        }

    def test_feature_not_breaking(self, entries):
        assert entries["feature"].is_breaking() is False

    def test_fix_not_breaking(self, entries):
        assert entries["fix"].is_breaking() is False

    def test_breaking_is_breaking(self, entries):
        assert entries["breaking"].is_breaking() is True

    def test_fields_stored(self, mod):
        v = mod.SemanticVersion.parse("3.0.0")
        e = mod.ChangelogEntry(v, "FEATURE", "New feature", "2024-12-01")
        assert e.version == v
        assert e.change_type == "FEATURE"
        assert e.description == "New feature"
        assert e.date == "2024-12-01"

    def test_mutable_dataclass(self, mod):
        v = mod.SemanticVersion.parse("1.0.0")
        e = mod.ChangelogEntry(v, "FIX", "desc", "2024-01-01")
        e.description = "updated"
        assert e.description == "updated"


# ===========================================================================
# APIVersionManager — registration and deprecation
# ===========================================================================


class TestAPIVersionManagerRegistration:
    @pytest.fixture
    def manager_with_versions(self, mod):
        mgr = mod.APIVersionManager()
        for s in ["1.0.0", "1.1.0", "2.0.0"]:
            v = mod.SemanticVersion.parse(s)
            c = mod.APIContract("api", v, ["id"])
            mgr.register_version(v, c)
        return mgr

    def test_supported_versions_all_active(self, manager_with_versions, mod):
        supported = manager_with_versions.get_supported_versions()
        assert len(supported) == 3
        assert supported == sorted(supported)

    def test_register_immediately_deprecated(self, mod):
        mgr = mod.APIVersionManager()
        v = mod.SemanticVersion.parse("0.9.0")
        c = mod.APIContract("api", v, ["id"])
        mgr.register_version(v, c, deprecated=True)
        assert mgr.is_deprecated(v)
        assert v not in mgr.get_supported_versions()

    def test_deprecate_removes_from_supported(self, manager_with_versions, mod):
        v1 = mod.SemanticVersion.parse("1.0.0")
        manager_with_versions.deprecate(v1)
        assert v1 not in manager_with_versions.get_supported_versions()
        assert len(manager_with_versions.get_supported_versions()) == 2

    def test_is_deprecated_false_for_active(self, manager_with_versions, mod):
        v = mod.SemanticVersion.parse("1.1.0")
        assert manager_with_versions.is_deprecated(v) is False

    def test_is_deprecated_true_after_deprecate(self, manager_with_versions, mod):
        v = mod.SemanticVersion.parse("2.0.0")
        manager_with_versions.deprecate(v)
        assert manager_with_versions.is_deprecated(v) is True

    def test_deprecate_unknown_raises(self, mod):
        mgr = mod.APIVersionManager()
        v = mod.SemanticVersion.parse("9.9.9")
        with pytest.raises(KeyError):
            mgr.deprecate(v)

    def test_is_deprecated_unknown_returns_false(self, mod):
        mgr = mod.APIVersionManager()
        v = mod.SemanticVersion.parse("9.9.9")
        assert mgr.is_deprecated(v) is False

    def test_get_supported_versions_empty(self, mod):
        mgr = mod.APIVersionManager()
        assert mgr.get_supported_versions() == []


# ===========================================================================
# APIVersionManager — changelog
# ===========================================================================


class TestAPIVersionManagerChangelog:
    @pytest.fixture
    def manager_with_changelog(self, mod):
        mgr = mod.APIVersionManager()
        entries = [
            mod.ChangelogEntry(mod.SemanticVersion.parse("1.0.0"), "FEATURE", "Initial", "2024-01-01"),
            mod.ChangelogEntry(mod.SemanticVersion.parse("1.1.0"), "FIX", "Fix bug", "2024-02-01"),
            mod.ChangelogEntry(mod.SemanticVersion.parse("1.2.0"), "FEATURE", "New endpoint", "2024-03-01"),
            mod.ChangelogEntry(mod.SemanticVersion.parse("2.0.0"), "BREAKING", "Major overhaul", "2024-06-01"),
            mod.ChangelogEntry(mod.SemanticVersion.parse("2.1.0"), "FIX", "Patch fix", "2024-07-01"),
        ]
        for e in entries:
            mgr.add_changelog_entry(e)
        return mgr

    def test_get_changelog_all_entries(self, manager_with_changelog):
        entries = manager_with_changelog.get_changelog()
        assert len(entries) == 5

    def test_get_changelog_since_v1_1(self, manager_with_changelog, mod):
        since = mod.SemanticVersion.parse("1.1.0")
        entries = manager_with_changelog.get_changelog(since)
        assert all(e.version >= since for e in entries)
        assert len(entries) == 4  # 1.1.0, 1.2.0, 2.0.0, 2.1.0

    def test_get_changelog_since_v2(self, manager_with_changelog, mod):
        since = mod.SemanticVersion.parse("2.0.0")
        entries = manager_with_changelog.get_changelog(since)
        assert len(entries) == 2  # 2.0.0, 2.1.0

    def test_get_changelog_since_future_empty(self, manager_with_changelog, mod):
        since = mod.SemanticVersion.parse("9.0.0")
        assert manager_with_changelog.get_changelog(since) == []

    def test_get_breaking_changes_all(self, manager_with_changelog):
        breaking = manager_with_changelog.get_breaking_changes()
        assert len(breaking) == 1
        assert breaking[0].change_type == "BREAKING"

    def test_get_breaking_changes_since_excludes_earlier(self, manager_with_changelog, mod):
        since = mod.SemanticVersion.parse("2.0.0")
        breaking = manager_with_changelog.get_breaking_changes(since)
        assert len(breaking) == 1

    def test_get_breaking_changes_since_after_all_breaking_empty(self, manager_with_changelog, mod):
        since = mod.SemanticVersion.parse("2.1.0")
        breaking = manager_with_changelog.get_breaking_changes(since)
        assert breaking == []

    def test_changelog_empty_by_default(self, mod):
        mgr = mod.APIVersionManager()
        assert mgr.get_changelog() == []

    def test_breaking_changes_empty_when_none(self, mod):
        mgr = mod.APIVersionManager()
        mgr.add_changelog_entry(
            mod.ChangelogEntry(mod.SemanticVersion.parse("1.0.0"), "FEATURE", "initial", "2024-01-01")
        )
        assert mgr.get_breaking_changes() == []

    def test_get_changelog_returns_copy(self, mod):
        mgr = mod.APIVersionManager()
        mgr.add_changelog_entry(mod.ChangelogEntry(mod.SemanticVersion.parse("1.0.0"), "FIX", "fix", "2024-01-01"))
        result = mgr.get_changelog()
        result.clear()
        assert len(mgr.get_changelog()) == 1
