"""
41_api_versioning_patterns.py — API Versioning and Contract Testing Patterns

Pure-Python implementation of five API versioning and contract testing
primitives using only the standard library (``dataclasses``, ``typing``).
No external dependencies are required.

    Pattern 1 — SemanticVersion:
                Immutable semantic version value object.  A
                ``@dataclass(frozen=True)`` that parses and represents a
                ``MAJOR.MINOR.PATCH`` version string per the SemVer 2.0.0
                specification.  :meth:`parse` is a class-method factory that
                accepts a dotted-string (e.g. ``"2.3.1"``) and raises
                :class:`ValueError` for malformed input.  :meth:`is_compatible_with`
                returns ``True`` when both versions share the same major
                number and *self* has an equal or higher minor version —
                meaning a consumer requesting ``other`` can be safely served
                by ``self``.  :meth:`is_breaking_change_from` returns ``True``
                when ``self.major > other.major`` — a hard signal that
                clients on *other* cannot use *self* without migration.
                Full rich-comparison support (``__lt__``, ``__le__``,
                ``__gt__``, ``__ge__``) enables sorting and range queries
                across version collections.  ``__str__`` reproduces the
                canonical ``"1.2.3"`` form.

    Pattern 2 — APIContract:
                Defines the observable surface of a versioned API endpoint:
                which request fields are *required* vs *optional*, and which
                response fields are expected.  :meth:`validate_request`
                accepts a payload dict and returns ``(True, [])`` when all
                required fields are present, or ``(False, [missing…])``
                listing every absent required field.
                :meth:`validate_response` checks that every field in an
                *expected_fields* list appears in the response payload.
                :meth:`is_backward_compatible_with` compares *self* against
                an *older_contract* and returns ``True`` only when every
                field that was required in the older version is still
                required in the current version — guaranteeing that
                existing clients will not break.

    Pattern 3 — VersionRouter:
                Dispatches incoming API requests to the correct versioned
                handler callable.  Handlers are registered with
                :meth:`register` keyed by :class:`SemanticVersion`.
                :meth:`route` first attempts an exact version match; if none
                is found it falls back to the highest registered version that
                is compatible with the requested version (same major,
                handler minor ≥ requested minor).  A :class:`ValueError` is
                raised when no compatible handler exists.
                :meth:`available_versions` returns all registered versions in
                ascending sort order.  :meth:`latest_version` returns the
                highest registered version.

    Pattern 4 — ChangelogEntry:
                Structured record of a single API change.  A plain
                ``@dataclass`` (mutable, for easy construction) with fields
                ``version: SemanticVersion``, ``change_type: str`` (one of
                ``"BREAKING"``, ``"FEATURE"``, or ``"FIX"``),
                ``description: str``, and ``date: str`` (ISO-8601 format
                recommended).  :meth:`is_breaking` returns ``True`` when
                ``change_type == "BREAKING"``, providing a convenient
                predicate for consumers that only care about non-backward-
                compatible changes.

    Pattern 5 — APIVersionManager:
                Lifecycle manager for a collection of versioned API
                contracts.  :meth:`register_version` stores a contract and
                an optional ``deprecated`` flag.  :meth:`deprecate` marks a
                registered version as deprecated post-hoc.
                :meth:`is_deprecated` returns the current deprecation status.
                :meth:`get_supported_versions` returns only the non-deprecated
                versions in ascending order.  :meth:`add_changelog_entry`
                appends a :class:`ChangelogEntry` to an internal log.
                :meth:`get_changelog` returns the full changelog, or only
                entries whose version is >= *since_version* when that
                argument is provided.  :meth:`get_breaking_changes` narrows
                the result further to ``BREAKING`` entries only.

Usage example
-------------
::

    # Parse and compare versions
    v1 = SemanticVersion.parse("1.0.0")
    v2 = SemanticVersion.parse("2.0.0")
    assert v2.is_breaking_change_from(v1)

    # Define and validate a contract
    contract = APIContract(
        name="users",
        version=v1,
        required_fields=["user_id", "email"],
        optional_fields=["phone"],
    )
    valid, missing = contract.validate_request({"user_id": "u1", "email": "a@b.com"})
    assert valid

    # Route a request to the right handler
    router = VersionRouter()
    router.register(v1, lambda payload: {"status": "ok", "version": "1.0.0"})
    result = router.route("1.0.0", {"user_id": "u1"})
    assert result["version"] == "1.0.0"

    # Track changes
    manager = APIVersionManager()
    manager.register_version(v1, contract)
    manager.add_changelog_entry(
        ChangelogEntry(v1, "FEATURE", "Initial release", "2024-01-01")
    )
    assert len(manager.get_changelog()) == 1

No external dependencies required.

Run:
    python examples/41_api_versioning_patterns.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ===========================================================================
# Pattern 1 — SemanticVersion
# ===========================================================================


@dataclass(frozen=True)
class SemanticVersion:
    """Immutable semantic version value object (MAJOR.MINOR.PATCH).

    Example::

        v = SemanticVersion.parse("2.1.0")
        assert str(v) == "2.1.0"
        assert v.is_compatible_with(SemanticVersion.parse("2.0.0"))
    """

    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, version_str: str) -> "SemanticVersion":
        """Parse a ``"MAJOR.MINOR.PATCH"`` string into a :class:`SemanticVersion`.

        Args:
            version_str: Dotted version string, e.g. ``"1.2.3"``.

        Returns:
            A new :class:`SemanticVersion` instance.

        Raises:
            ValueError: When *version_str* is not in the expected format or
                any component is not a non-negative integer.
        """
        if not isinstance(version_str, str):
            raise ValueError(f"version_str must be a string, got {type(version_str)!r}")
        parts = version_str.strip().split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid semantic version '{version_str}': expected MAJOR.MINOR.PATCH")
        try:
            major, minor, patch = (int(p) for p in parts)
        except ValueError:
            raise ValueError(f"Invalid semantic version '{version_str}': all components must be integers")
        if major < 0 or minor < 0 or patch < 0:
            raise ValueError(f"Invalid semantic version '{version_str}': components must be non-negative")
        return cls(major=major, minor=minor, patch=patch)

    def is_compatible_with(self, other: "SemanticVersion") -> bool:
        """Return ``True`` when *self* can serve a consumer built against *other*.

        Compatibility requires the same major version and ``self.minor >=
        other.minor`` — backward-compatible additions to minor versions are
        safe for existing consumers.

        Args:
            other: The version the consumer was built against.

        Returns:
            ``True`` when *self* is a safe drop-in for *other*.
        """
        return self.major == other.major and self.minor >= other.minor

    def is_breaking_change_from(self, other: "SemanticVersion") -> bool:
        """Return ``True`` when *self* introduces a breaking change relative to *other*.

        A major-version bump is the canonical SemVer signal for
        backward-incompatible changes.

        Args:
            other: The baseline version.

        Returns:
            ``True`` when ``self.major > other.major``.
        """
        return self.major > other.major

    # ------------------------------------------------------------------
    # Comparison helpers
    # ------------------------------------------------------------------

    def _as_tuple(self) -> tuple[int, int, int]:
        return (self.major, self.minor, self.patch)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._as_tuple() < other._as_tuple()

    def __le__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._as_tuple() <= other._as_tuple()

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._as_tuple() > other._as_tuple()

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._as_tuple() >= other._as_tuple()

    def __str__(self) -> str:  # pragma: no cover — trivial
        return f"{self.major}.{self.minor}.{self.patch}"


# ===========================================================================
# Pattern 2 — APIContract
# ===========================================================================


class APIContract:
    """Defines the observable surface of a versioned API endpoint.

    Example::

        contract = APIContract(
            name="orders",
            version=SemanticVersion.parse("1.0.0"),
            required_fields=["order_id", "amount"],
            optional_fields=["note"],
        )
        valid, missing = contract.validate_request({"order_id": "o1", "amount": 50})
        assert valid
    """

    def __init__(
        self,
        name: str,
        version: SemanticVersion,
        required_fields: list[str],
        optional_fields: list[str] | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self.required_fields: list[str] = list(required_fields)
        self.optional_fields: list[str] = list(optional_fields) if optional_fields else []

    def validate_request(self, payload: dict[str, Any]) -> tuple[bool, list[str]]:
        """Check that all required request fields are present in *payload*.

        Args:
            payload: The incoming request body as a dict.

        Returns:
            A ``(is_valid, missing_fields)`` tuple.  When *is_valid* is
            ``True`` the second element is an empty list.
        """
        missing = [f for f in self.required_fields if f not in payload]
        return (len(missing) == 0, missing)

    def validate_response(self, payload: dict[str, Any], expected_fields: list[str]) -> tuple[bool, list[str]]:
        """Check that all *expected_fields* are present in a response *payload*.

        Args:
            payload: The response body returned by the API handler.
            expected_fields: Fields the consumer relies on being present.

        Returns:
            A ``(is_valid, missing_fields)`` tuple.
        """
        missing = [f for f in expected_fields if f not in payload]
        return (len(missing) == 0, missing)

    def is_backward_compatible_with(self, older_contract: "APIContract") -> bool:
        """Return ``True`` when *self* is backward-compatible with *older_contract*.

        Backward compatibility is maintained as long as every field that was
        *required* in the older contract is still required in *self*.  Removing
        a previously required field would break existing clients.

        Args:
            older_contract: The contract from a previous API version.

        Returns:
            ``True`` when all of *older_contract*'s required fields are present
            in *self*'s required fields.
        """
        older_required = set(older_contract.required_fields)
        self_required = set(self.required_fields)
        return older_required.issubset(self_required)

    def __repr__(self) -> str:  # pragma: no cover
        return f"APIContract(name={self.name!r}, version={self.version})"


# ===========================================================================
# Pattern 3 — VersionRouter
# ===========================================================================


class VersionRouter:
    """Dispatches requests to the correct versioned API handler.

    Example::

        router = VersionRouter()
        router.register(SemanticVersion.parse("1.0.0"), lambda p: "v1")
        router.register(SemanticVersion.parse("1.1.0"), lambda p: "v1.1")
        assert router.route("1.0.0", {}) == "v1"
        assert router.route("1.1.0", {}) == "v1.1"
    """

    def __init__(self) -> None:
        self._handlers: dict[SemanticVersion, Any] = {}

    def register(self, version: SemanticVersion, handler: Any) -> None:
        """Register a callable *handler* for the given *version*.

        Args:
            version: The :class:`SemanticVersion` this handler implements.
            handler: Any callable that accepts a ``dict`` payload and returns
                a result.
        """
        self._handlers[version] = handler

    def route(self, version_str: str, payload: dict[str, Any]) -> Any:
        """Route *payload* to the handler best matching *version_str*.

        Resolution order:

        1. Exact version match.
        2. Highest compatible version (same major, handler minor ≥ requested).

        Args:
            version_str: Requested version string, e.g. ``"1.2.0"``.
            payload: Request payload dict forwarded to the handler.

        Returns:
            Whatever the matched handler returns.

        Raises:
            ValueError: When no registered version is compatible with the
                requested version.
        """
        requested = SemanticVersion.parse(version_str)

        # 1. Exact match
        if requested in self._handlers:
            return self._handlers[requested](payload)

        # 2. Highest compatible version
        compatible = sorted(
            (v for v in self._handlers if v.is_compatible_with(requested)),
            reverse=True,
        )
        if compatible:
            best = compatible[0]
            return self._handlers[best](payload)

        raise ValueError(f"No handler registered for version {version_str!r}")

    def available_versions(self) -> list[SemanticVersion]:
        """Return all registered versions in ascending order.

        Returns:
            Sorted list of :class:`SemanticVersion` objects.
        """
        return sorted(self._handlers.keys())

    def latest_version(self) -> SemanticVersion:
        """Return the highest registered version.

        Returns:
            The maximum :class:`SemanticVersion` among all registered handlers.

        Raises:
            ValueError: When no handlers have been registered.
        """
        if not self._handlers:
            raise ValueError("No versions registered")
        return max(self._handlers.keys())


# ===========================================================================
# Pattern 4 — ChangelogEntry
# ===========================================================================


@dataclass
class ChangelogEntry:
    """Structured record of a single API change.

    Example::

        entry = ChangelogEntry(
            version=SemanticVersion.parse("2.0.0"),
            change_type="BREAKING",
            description="Removed deprecated /v1/users endpoint",
            date="2024-06-01",
        )
        assert entry.is_breaking()
    """

    version: SemanticVersion
    change_type: str  # "BREAKING" | "FEATURE" | "FIX"
    description: str
    date: str

    def is_breaking(self) -> bool:
        """Return ``True`` when this entry represents a breaking change.

        Returns:
            ``True`` iff ``change_type == "BREAKING"``.
        """
        return self.change_type == "BREAKING"


# ===========================================================================
# Pattern 5 — APIVersionManager
# ===========================================================================


class APIVersionManager:
    """Manages the full lifecycle of versioned API contracts.

    Example::

        manager = APIVersionManager()
        v1 = SemanticVersion.parse("1.0.0")
        contract_v1 = APIContract("users", v1, ["user_id"])
        manager.register_version(v1, contract_v1)
        assert v1 in [v for v in manager.get_supported_versions()]
        manager.deprecate(v1)
        assert manager.get_supported_versions() == []
    """

    def __init__(self) -> None:
        self._contracts: dict[SemanticVersion, APIContract] = {}
        self._deprecated: set[SemanticVersion] = set()
        self._changelog: list[ChangelogEntry] = []

    def register_version(
        self,
        version: SemanticVersion,
        contract: APIContract,
        deprecated: bool = False,
    ) -> None:
        """Register a contract for *version*.

        Args:
            version: The :class:`SemanticVersion` being registered.
            contract: The :class:`APIContract` describing this version's surface.
            deprecated: When ``True`` the version is immediately marked
                deprecated and excluded from :meth:`get_supported_versions`.
        """
        self._contracts[version] = contract
        if deprecated:
            self._deprecated.add(version)

    def add_changelog_entry(self, entry: ChangelogEntry) -> None:
        """Append a :class:`ChangelogEntry` to the internal log.

        Args:
            entry: The changelog record to append.
        """
        self._changelog.append(entry)

    def is_deprecated(self, version: SemanticVersion) -> bool:
        """Return ``True`` when *version* has been deprecated.

        Args:
            version: The version to check.

        Returns:
            ``True`` if deprecated, ``False`` otherwise (including unknown versions).
        """
        return version in self._deprecated

    def deprecate(self, version: SemanticVersion) -> None:
        """Mark *version* as deprecated.

        Args:
            version: The version to deprecate.

        Raises:
            KeyError: When *version* has not been registered.
        """
        if version not in self._contracts:
            raise KeyError(f"Version {version} is not registered")
        self._deprecated.add(version)

    def get_supported_versions(self) -> list[SemanticVersion]:
        """Return all non-deprecated registered versions in ascending order.

        Returns:
            Sorted list of active :class:`SemanticVersion` objects.
        """
        return sorted(v for v in self._contracts if v not in self._deprecated)

    def get_changelog(self, since_version: SemanticVersion | None = None) -> list[ChangelogEntry]:
        """Return changelog entries, optionally filtered to >= *since_version*.

        Args:
            since_version: When provided only entries whose ``version`` is
                greater than or equal to *since_version* are returned.

        Returns:
            Ordered list of :class:`ChangelogEntry` objects.
        """
        if since_version is None:
            return list(self._changelog)
        return [e for e in self._changelog if e.version >= since_version]

    def get_breaking_changes(self, since_version: SemanticVersion | None = None) -> list[ChangelogEntry]:
        """Return only ``BREAKING`` changelog entries, optionally filtered.

        Args:
            since_version: When provided only entries whose ``version`` is
                >= *since_version* are considered.

        Returns:
            List of :class:`ChangelogEntry` objects with ``change_type == "BREAKING"``.
        """
        return [e for e in self.get_changelog(since_version) if e.is_breaking()]


# ===========================================================================
# Demonstration
# ===========================================================================

if __name__ == "__main__":  # pragma: no cover
    print("=== Pattern 1: SemanticVersion ===")
    v1 = SemanticVersion.parse("1.0.0")
    v1_1 = SemanticVersion.parse("1.1.0")
    v2 = SemanticVersion.parse("2.0.0")
    print(f"  v1={v1}, v1_1={v1_1}, v2={v2}")
    print(f"  v1_1 compatible with v1: {v1_1.is_compatible_with(v1)}")
    print(f"  v2 breaking from v1: {v2.is_breaking_change_from(v1)}")
    print(f"  sorted: {sorted([v2, v1_1, v1])}")

    print("\n=== Pattern 2: APIContract ===")
    contract_v1 = APIContract("users", v1, ["user_id", "email"], ["phone"])
    valid, missing = contract_v1.validate_request({"user_id": "u1", "email": "a@b.com"})
    print(f"  valid request: {valid}, missing: {missing}")
    valid2, missing2 = contract_v1.validate_request({"user_id": "u1"})
    print(f"  invalid request: {valid2}, missing: {missing2}")

    contract_v1_1 = APIContract("users", v1_1, ["user_id", "email", "name"], ["phone"])
    print(f"  v1.1 backward compat with v1: {contract_v1_1.is_backward_compatible_with(contract_v1)}")

    print("\n=== Pattern 3: VersionRouter ===")
    router = VersionRouter()
    router.register(v1, lambda p: {"version": "1.0.0", "data": p})
    router.register(v1_1, lambda p: {"version": "1.1.0", "data": p})
    result = router.route("1.0.0", {"user_id": "u1"})
    print(f"  routed 1.0.0: {result['version']}")
    result2 = router.route("1.1.0", {"user_id": "u2"})
    print(f"  routed 1.1.0: {result2['version']}")
    print(f"  latest: {router.latest_version()}")

    print("\n=== Pattern 4: ChangelogEntry ===")
    entry1 = ChangelogEntry(v1, "FEATURE", "Initial release", "2024-01-01")
    entry2 = ChangelogEntry(v2, "BREAKING", "Removed legacy fields", "2024-06-01")
    print(f"  entry1 breaking: {entry1.is_breaking()}")
    print(f"  entry2 breaking: {entry2.is_breaking()}")

    print("\n=== Pattern 5: APIVersionManager ===")
    manager = APIVersionManager()
    manager.register_version(v1, contract_v1)
    manager.register_version(v1_1, contract_v1_1)
    manager.register_version(v2, APIContract("users", v2, ["user_id", "email", "name"]), deprecated=True)
    manager.add_changelog_entry(entry1)
    manager.add_changelog_entry(entry2)
    print(f"  supported: {manager.get_supported_versions()}")
    print(f"  v2 deprecated: {manager.is_deprecated(v2)}")
    print(f"  changelog since v2: {manager.get_changelog(v2)}")
    print(f"  breaking changes: {manager.get_breaking_changes()}")
    print("\nAll patterns demonstrated successfully.")
