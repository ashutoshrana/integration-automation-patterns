"""
10_schema_evolution.py — Schema evolution and consumer version compatibility
for enterprise event-driven integration pipelines.

Demonstrates the patterns required to evolve event schemas across a
distributed system where producers and consumers are deployed independently
at different versions — one of the most common sources of silent data
corruption in enterprise integration.

    Pattern 1 — FieldEvolutionRule: Classifies how each field change
                affects backward and forward compatibility. NEW_OPTIONAL_FIELD
                and REQUIRED_TO_OPTIONAL are fully compatible. FIELD_RENAMED
                and TYPE_CHANGED are breaking changes that require explicit
                migration mapping.

    Pattern 2 — SchemaVersion: Semantic version (major.minor.patch) with
                explicit compatibility contracts. Minor version bumps must be
                backward compatible. Major version bumps may be breaking.
                Consumers at different versions can read events from any
                compatible producer version.

    Pattern 3 — SchemaRegistry: Central schema store. Registers schema
                versions, validates events against their schema, and provides
                the correct reader schema for cross-version reads.

    Pattern 4 — EventMigrator: Transforms events from one schema version to
                another by applying migration rules in sequence. Supports
                multi-step migrations (v1.0 → v1.1 → v2.0).

    Pattern 5 — MultiVersionFanout: Delivers events from a single producer
                to consumers at N different schema versions, applying the
                correct migration for each consumer version group.

Scenarios
---------

  A. Backward compatible evolution (v1.0 → v1.1): producer adds optional
     field 'opportunity_score'. Consumers at v1.0 read v1.1 events without
     errors — optional fields have defaults. v1.1 consumers see new field.

  B. Forward compatible read (v1.1 consumer reads v1.0 event): consumer
     at v1.1 reads an event produced at v1.0 — missing new field populated
     with default. No error.

  C. Breaking change (v1.x → v2.0): field type change requires explicit
     migration. EventMigrator transforms the event before delivery.
     Multi-step migration (v1.0 → v1.2 → v2.0) demonstrated.

  D. Multi-version fanout: producer at v1.2 delivers to consumers at v1.0,
     v1.1, v1.2, and v2.0. Each consumer version group receives correctly
     migrated events.

No external dependencies required.

Run:
    python examples/10_schema_evolution.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Field evolution rules
# ---------------------------------------------------------------------------

class FieldEvolutionRule(str, Enum):
    """
    How a field change affects backward and forward compatibility.

    Backward compatible: older consumers can read newer events.
    Forward compatible: newer consumers can read older events.
    Breaking: requires explicit migration — major version bump.
    """
    NEW_OPTIONAL_FIELD = "new_optional"         # Both backward and forward compatible
    REQUIRED_TO_OPTIONAL = "required_to_optional"  # Backward compatible (old consumers ignore default)
    FIELD_DEPRECATED = "deprecated"             # Backward compatible — field still present
    FIELD_RENAMED = "renamed"                   # Breaking — old consumers see null/missing
    FIELD_REMOVED = "removed"                   # Forward compatible only
    TYPE_CHANGED = "type_changed"               # Breaking — silent data corruption risk
    NEW_REQUIRED_FIELD = "new_required"         # Breaking — old consumers cannot produce


class CompatibilityMode(str, Enum):
    BACKWARD = "backward"       # New schema can read old events
    FORWARD = "forward"         # Old schema can read new events
    FULL = "full"               # Both backward and forward compatible
    NONE = "none"               # Breaking — requires explicit migration


# ---------------------------------------------------------------------------
# Schema version
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SchemaVersion:
    """
    Semantic version with explicit compatibility contracts.

    Minor version increments must be backward compatible (consumers at older
    minor versions can read events from newer minor versions, with defaults
    for new optional fields).

    Major version increments are permitted to be breaking — require explicit
    EventMigrator mapping.
    """
    major: int
    minor: int
    patch: int = 0

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}.{self.patch}"

    def is_same_major(self, other: SchemaVersion) -> bool:
        return self.major == other.major

    def is_backward_compatible_with(self, older: SchemaVersion) -> bool:
        """Can THIS (newer) schema read events produced by OLDER schema?"""
        if not self.is_same_major(older):
            return False  # Major version crossing requires migration
        return self >= older  # Same major: newer schema reads older events (forward compat)

    def is_forward_compatible_with(self, newer: SchemaVersion) -> bool:
        """Can THIS (older) schema read events produced by NEWER schema?"""
        if not self.is_same_major(newer):
            return False
        return self <= newer  # Same major: older schema reads newer events (backward compat)

    def __lt__(self, other: SchemaVersion) -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other: SchemaVersion) -> bool:
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __gt__(self, other: SchemaVersion) -> bool:
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    def __ge__(self, other: SchemaVersion) -> bool:
        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

@dataclass
class FieldSchema:
    """Definition of a single field in an event schema."""
    name: str
    field_type: str               # "str", "float", "int", "bool", "datetime", "dict"
    required: bool = True
    default: Any = None
    description: str = ""
    deprecated_since: SchemaVersion | None = None


@dataclass
class EventSchema:
    """
    Schema for a versioned event type.

    The field list is the authoritative definition — validators use it to
    check event payloads for missing required fields and unknown fields.
    """
    event_type: str
    version: SchemaVersion
    fields: list[FieldSchema]
    changes_from_previous: list[tuple[str, FieldEvolutionRule]] = field(default_factory=list)

    def validate(self, event: dict) -> tuple[bool, list[str]]:
        """Validate an event payload against this schema. Returns (valid, errors)."""
        errors: list[str] = []
        payload = event.get("payload", event)

        for f in self.fields:
            if f.deprecated_since:
                continue  # Deprecated fields are optional
            if f.required and f.name not in payload:
                errors.append(f"Missing required field: '{f.name}' (type={f.field_type})")

        return len(errors) == 0, errors

    def apply_defaults(self, event: dict) -> dict:
        """Fill in default values for optional fields missing from an event."""
        payload = event.get("payload", event).copy()
        for f in self.fields:
            if not f.required and f.name not in payload and f.default is not None:
                payload[f.name] = f.default
        return {**event, "payload": payload} if "payload" in event else payload


# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------

class SchemaRegistry:
    """
    Central schema store for all event types and versions.

    Provides:
    - Registration of schema versions
    - Event validation against a specific version
    - Compatible reader resolution for cross-version reads
    """

    def __init__(self) -> None:
        self._schemas: dict[str, dict[str, EventSchema]] = {}

    def register(self, schema: EventSchema) -> None:
        key = schema.event_type
        if key not in self._schemas:
            self._schemas[key] = {}
        self._schemas[key][str(schema.version)] = schema

    def get(self, event_type: str, version: SchemaVersion) -> EventSchema | None:
        return self._schemas.get(event_type, {}).get(str(version))

    def validate(self, event: dict, schema_version: SchemaVersion) -> tuple[bool, list[str]]:
        event_type = event.get("event_type", "")
        schema = self.get(event_type, schema_version)
        if schema is None:
            return False, [f"No schema registered for {event_type} {schema_version}"]
        return schema.validate(event)

    def resolve_reader(
        self, event_type: str, producer_version: SchemaVersion, consumer_version: SchemaVersion
    ) -> tuple[str, list[str]]:
        """
        Determine how a consumer at consumer_version should read events from
        producer_version. Returns (compatibility_mode, notes).
        """
        if producer_version == consumer_version:
            return "exact_match", []

        if consumer_version.is_backward_compatible_with(producer_version):
            return "backward_compatible", [
                f"Consumer {consumer_version} can read {producer_version} events directly "
                f"(same major; newer consumer reads older producer events with defaults)"
            ]
        if consumer_version.is_forward_compatible_with(producer_version):
            return "forward_compatible", [
                f"Consumer {consumer_version} can read {producer_version} events directly "
                f"(same major; older consumer reads newer producer events, ignoring new optional fields)"
            ]
        return "migration_required", [
            f"Consumer {consumer_version} and producer {producer_version} span a major "
            f"version boundary — EventMigrator required"
        ]


# ---------------------------------------------------------------------------
# Migration rule and migrator
# ---------------------------------------------------------------------------

@dataclass
class MigrationRule:
    """A single field transformation applied during version migration."""
    field: str
    rule: FieldEvolutionRule
    new_field_name: str | None = None     # For FIELD_RENAMED
    transform: Any = None                  # Callable(old_value) -> new_value for TYPE_CHANGED
    default: Any = None                    # Default for NEW_REQUIRED_FIELD


@dataclass
class SchemaMigration:
    """Migration specification from one schema version to an adjacent version."""
    from_version: SchemaVersion
    to_version: SchemaVersion
    event_type: str
    rules: list[MigrationRule]

    def apply(self, event: dict) -> dict:
        """Apply migration rules to transform an event payload."""
        payload = event.get("payload", event).copy()

        for rule in self.rules:
            if rule.rule == FieldEvolutionRule.FIELD_RENAMED:
                if rule.field in payload and rule.new_field_name:
                    payload[rule.new_field_name] = payload.pop(rule.field)

            elif rule.rule == FieldEvolutionRule.TYPE_CHANGED and rule.transform:
                if rule.field in payload:
                    payload[rule.field] = rule.transform(payload[rule.field])

            elif rule.rule == FieldEvolutionRule.FIELD_REMOVED:
                payload.pop(rule.field, None)

            elif rule.rule == FieldEvolutionRule.NEW_REQUIRED_FIELD and rule.default is not None:
                if rule.field not in payload:
                    payload[rule.field] = rule.default

        migrated = {**event, "payload": payload, "schema_version": str(self.to_version)}
        if "payload" not in event:
            migrated = {**payload, "schema_version": str(self.to_version)}
        return migrated


class EventMigrator:
    """
    Transforms events between schema versions by applying migration chains.

    Supports multi-step migrations: v1.0 → v1.1 → v2.0. The migrator finds
    the shortest path from source to target version and applies each
    intermediate migration in sequence.
    """

    def __init__(self) -> None:
        self._migrations: dict[str, list[SchemaMigration]] = {}

    def register(self, migration: SchemaMigration) -> None:
        key = migration.event_type
        if key not in self._migrations:
            self._migrations[key] = []
        self._migrations[key].append(migration)

    def can_migrate(self, event_type: str, from_version: SchemaVersion, to_version: SchemaVersion) -> bool:
        """Check if a migration path exists (direct or via intermediate versions)."""
        return self._find_path(event_type, from_version, to_version) is not None

    def migrate(self, event: dict, from_version: SchemaVersion, to_version: SchemaVersion) -> dict:
        """Migrate event through the migration chain from from_version to to_version."""
        event_type = event.get("event_type", "")
        path = self._find_path(event_type, from_version, to_version)
        if path is None:
            raise ValueError(
                f"No migration path from {from_version} to {to_version} for {event_type}"
            )
        result = event
        for migration in path:
            result = migration.apply(result)
        return result

    def _find_path(
        self, event_type: str, from_version: SchemaVersion, to_version: SchemaVersion
    ) -> list[SchemaMigration] | None:
        """BFS to find shortest migration path."""
        migrations = self._migrations.get(event_type, [])
        # Build adjacency
        adj: dict[str, list[SchemaMigration]] = {}
        for m in migrations:
            k = str(m.from_version)
            if k not in adj:
                adj[k] = []
            adj[k].append(m)

        # BFS
        start = str(from_version)
        end = str(to_version)
        if start == end:
            return []
        queue: list[tuple[str, list[SchemaMigration]]] = [(start, [])]
        visited = {start}
        while queue:
            current, path = queue.pop(0)
            for m in adj.get(current, []):
                next_ver = str(m.to_version)
                new_path = path + [m]
                if next_ver == end:
                    return new_path
                if next_ver not in visited:
                    visited.add(next_ver)
                    queue.append((next_ver, new_path))
        return None


# ---------------------------------------------------------------------------
# Multi-version fanout
# ---------------------------------------------------------------------------

@dataclass
class ConsumerVersionGroup:
    """A group of consumers at the same schema version."""
    consumer_ids: list[str]
    version: SchemaVersion


class MultiVersionFanout:
    """
    Delivers events from a single producer to consumers at N different
    schema versions, applying the correct migration for each version group.

    This is the runtime counterpart to the SchemaRegistry — it uses the
    registry to determine compatibility and the migrator to transform events
    that need migration.
    """

    def __init__(self, registry: SchemaRegistry, migrator: EventMigrator) -> None:
        self._registry = registry
        self._migrator = migrator

    def fanout(
        self,
        event: dict,
        producer_version: SchemaVersion,
        consumer_groups: list[ConsumerVersionGroup],
    ) -> dict[str, dict]:
        """
        Deliver event to each consumer version group.

        Returns dict mapping consumer_version_str → delivered_event.
        """
        results: dict[str, dict] = {}
        event_type = event.get("event_type", "")

        for group in consumer_groups:
            consumer_version = group.version
            compat_mode, notes = self._registry.resolve_reader(
                event_type, producer_version, consumer_version
            )

            if compat_mode in ("exact_match", "forward_compatible"):
                # Consumer can read event directly — apply defaults for any new optional fields
                schema = self._registry.get(event_type, consumer_version)
                delivered = schema.apply_defaults(event) if schema else event
                results[str(consumer_version)] = {
                    "event": delivered,
                    "delivery_mode": compat_mode,
                    "consumers": group.consumer_ids,
                }
            elif compat_mode == "backward_compatible":
                # Consumer is newer — apply defaults for fields added between versions
                schema = self._registry.get(event_type, consumer_version)
                delivered = schema.apply_defaults(event) if schema else event
                results[str(consumer_version)] = {
                    "event": delivered,
                    "delivery_mode": compat_mode,
                    "consumers": group.consumer_ids,
                }
            elif compat_mode == "migration_required":
                if self._migrator.can_migrate(event_type, producer_version, consumer_version):
                    migrated = self._migrator.migrate(event, producer_version, consumer_version)
                    results[str(consumer_version)] = {
                        "event": migrated,
                        "delivery_mode": "migrated",
                        "consumers": group.consumer_ids,
                    }
                else:
                    results[str(consumer_version)] = {
                        "event": None,
                        "delivery_mode": "migration_failed",
                        "consumers": group.consumer_ids,
                        "error": f"No migration path from {producer_version} to {consumer_version}",
                    }

        return results


# ---------------------------------------------------------------------------
# Define OpportunityUpdated event schemas (Salesforce CRM pipeline)
# ---------------------------------------------------------------------------

V1_0 = SchemaVersion(1, 0)
V1_1 = SchemaVersion(1, 1)
V1_2 = SchemaVersion(1, 2)
V2_0 = SchemaVersion(2, 0)

EVENT_TYPE = "OpportunityUpdated"


def build_registry_and_migrator() -> tuple[SchemaRegistry, EventMigrator]:
    registry = SchemaRegistry()

    # v1.0 — original schema
    schema_v1_0 = EventSchema(
        event_type=EVENT_TYPE,
        version=V1_0,
        fields=[
            FieldSchema("opportunity_id", "str", required=True),
            FieldSchema("account_id", "str", required=True),
            FieldSchema("stage", "str", required=True),
            FieldSchema("amount", "float", required=True),
            FieldSchema("close_date", "str", required=True),
            FieldSchema("owner_id", "str", required=True),
        ],
    )

    # v1.1 — adds optional 'opportunity_score' (backward+forward compatible)
    schema_v1_1 = EventSchema(
        event_type=EVENT_TYPE,
        version=V1_1,
        fields=[
            FieldSchema("opportunity_id", "str", required=True),
            FieldSchema("account_id", "str", required=True),
            FieldSchema("stage", "str", required=True),
            FieldSchema("amount", "float", required=True),
            FieldSchema("close_date", "str", required=True),
            FieldSchema("owner_id", "str", required=True),
            FieldSchema("opportunity_score", "float", required=False, default=0.0,
                        description="ML-predicted win probability [0.0, 1.0]"),
        ],
        changes_from_previous=[("opportunity_score", FieldEvolutionRule.NEW_OPTIONAL_FIELD)],
    )

    # v1.2 — adds optional 'region' field (backward+forward compatible)
    schema_v1_2 = EventSchema(
        event_type=EVENT_TYPE,
        version=V1_2,
        fields=[
            FieldSchema("opportunity_id", "str", required=True),
            FieldSchema("account_id", "str", required=True),
            FieldSchema("stage", "str", required=True),
            FieldSchema("amount", "float", required=True),
            FieldSchema("close_date", "str", required=True),
            FieldSchema("owner_id", "str", required=True),
            FieldSchema("opportunity_score", "float", required=False, default=0.0),
            FieldSchema("region", "str", required=False, default="UNKNOWN",
                        description="Sales region code"),
        ],
        changes_from_previous=[("region", FieldEvolutionRule.NEW_OPTIONAL_FIELD)],
    )

    # v2.0 — BREAKING: 'amount' renamed to 'amount_usd'; 'owner_id' renamed to 'rep_id'
    schema_v2_0 = EventSchema(
        event_type=EVENT_TYPE,
        version=V2_0,
        fields=[
            FieldSchema("opportunity_id", "str", required=True),
            FieldSchema("account_id", "str", required=True),
            FieldSchema("stage", "str", required=True),
            FieldSchema("amount_usd", "float", required=True,
                        description="Deal amount in USD (renamed from 'amount' in v2.0)"),
            FieldSchema("close_date", "str", required=True),
            FieldSchema("rep_id", "str", required=True,
                        description="Sales rep ID (renamed from 'owner_id' in v2.0)"),
            FieldSchema("opportunity_score", "float", required=False, default=0.0),
            FieldSchema("region", "str", required=False, default="UNKNOWN"),
        ],
        changes_from_previous=[
            ("amount", FieldEvolutionRule.FIELD_RENAMED),     # → amount_usd
            ("owner_id", FieldEvolutionRule.FIELD_RENAMED),   # → rep_id
        ],
    )

    for schema in [schema_v1_0, schema_v1_1, schema_v1_2, schema_v2_0]:
        registry.register(schema)

    # Migration chain: v1.0 → v1.1 (add score default)
    migration_v1_0_to_v1_1 = SchemaMigration(
        from_version=V1_0, to_version=V1_1, event_type=EVENT_TYPE,
        rules=[
            MigrationRule("opportunity_score", FieldEvolutionRule.NEW_OPTIONAL_FIELD, default=0.0),
        ],
    )
    # v1.1 → v1.2 (add region default)
    migration_v1_1_to_v1_2 = SchemaMigration(
        from_version=V1_1, to_version=V1_2, event_type=EVENT_TYPE,
        rules=[
            MigrationRule("region", FieldEvolutionRule.NEW_OPTIONAL_FIELD, default="UNKNOWN"),
        ],
    )
    # v1.2 → v2.0 (rename amount → amount_usd; rename owner_id → rep_id)
    migration_v1_2_to_v2_0 = SchemaMigration(
        from_version=V1_2, to_version=V2_0, event_type=EVENT_TYPE,
        rules=[
            MigrationRule("amount", FieldEvolutionRule.FIELD_RENAMED, new_field_name="amount_usd"),
            MigrationRule("owner_id", FieldEvolutionRule.FIELD_RENAMED, new_field_name="rep_id"),
        ],
    )

    migrator = EventMigrator()
    for migration in [migration_v1_0_to_v1_1, migration_v1_1_to_v1_2, migration_v1_2_to_v2_0]:
        migrator.register(migration)

    return registry, migrator


# ---------------------------------------------------------------------------
# Main — four scenarios
# ---------------------------------------------------------------------------

def main() -> None:
    registry, migrator = build_registry_and_migrator()

    # Canonical v1.0 event
    event_v1_0 = {
        "event_id": "evt-001",
        "event_type": EVENT_TYPE,
        "schema_version": "v1.0.0",
        "produced_at": "2026-04-13T10:00:00Z",
        "payload": {
            "opportunity_id": "OPP-2026-001",
            "account_id": "ACC-4471",
            "stage": "Proposal",
            "amount": 125000.0,
            "close_date": "2026-06-30",
            "owner_id": "rep-sarah-k",
        },
    }

    # v1.1 event with opportunity_score
    event_v1_1 = {
        **event_v1_0,
        "schema_version": "v1.1.0",
        "payload": {**event_v1_0["payload"], "opportunity_score": 0.73},
    }

    print("=" * 72)
    print("SCHEMA EVOLUTION — OpportunityUpdated (Salesforce CRM Pipeline)")
    print("=" * 72)
    print(f"Versions registered: v1.0.0, v1.1.0, v1.2.0, v2.0.0")
    print(f"Breaking change: v2.0 renames 'amount'→'amount_usd', 'owner_id'→'rep_id'")

    # ------------------------------------------------------------------
    # Scenario A: Backward compatible evolution — v1.0 consumer reads v1.1 event
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("Scenario A: Backward compatible (v1.0 consumer reads v1.1 event)")
    print("  v1.1 adds optional 'opportunity_score'. v1.0 consumers ignore it.")
    print("=" * 72)

    compat_mode_a, notes_a = registry.resolve_reader(EVENT_TYPE, V1_1, V1_0)
    print(f"  Compatibility: {compat_mode_a}")
    for note in notes_a:
        print(f"  Note: {note}")

    valid, errors = registry.validate(event_v1_1, V1_0)
    print(f"  v1.0 validation of v1.1 event: {'PASS' if valid else 'FAIL'} (errors: {errors})")

    # v1.0 consumer reads v1.1 event — opportunity_score is just ignored
    v1_0_payload = {k: v for k, v in event_v1_1["payload"].items() if k != "opportunity_score"}
    print(f"  v1.0 consumer payload (opportunity_score stripped): {v1_0_payload}")

    # ------------------------------------------------------------------
    # Scenario B: Forward compatible read — v1.1 consumer reads v1.0 event
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("Scenario B: Forward compatible (v1.1 consumer reads v1.0 event)")
    print("  v1.0 event missing 'opportunity_score'. Consumer applies default=0.0.")
    print("=" * 72)

    compat_mode_b, notes_b = registry.resolve_reader(EVENT_TYPE, V1_0, V1_1)
    print(f"  Compatibility: {compat_mode_b}")

    schema_v1_1 = registry.get(EVENT_TYPE, V1_1)
    event_with_defaults = schema_v1_1.apply_defaults(event_v1_0)
    payload_with_defaults = event_with_defaults.get("payload", event_with_defaults)
    print(f"  After applying v1.1 defaults: opportunity_score={payload_with_defaults.get('opportunity_score')}")
    print(f"  Full payload: {payload_with_defaults}")

    # ------------------------------------------------------------------
    # Scenario C: Breaking change — v1.0 → v2.0 via migration chain
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("Scenario C: Breaking change (v1.0 → v2.0 requires multi-step migration)")
    print("  Migration chain: v1.0 → v1.1 → v1.2 → v2.0")
    print("  Transforms: amount→amount_usd, owner_id→rep_id")
    print("=" * 72)

    compat_mode_c, notes_c = registry.resolve_reader(EVENT_TYPE, V1_0, V2_0)
    print(f"  Compatibility: {compat_mode_c}")

    can_migrate = migrator.can_migrate(EVENT_TYPE, V1_0, V2_0)
    print(f"  Can migrate v1.0 → v2.0: {can_migrate}")

    migrated_event = migrator.migrate(event_v1_0, V1_0, V2_0)
    migrated_payload = migrated_event.get("payload", migrated_event)
    print(f"  Original v1.0 payload: amount={event_v1_0['payload']['amount']}, owner_id={event_v1_0['payload']['owner_id']}")
    print(f"  Migrated v2.0 payload: amount_usd={migrated_payload.get('amount_usd')}, rep_id={migrated_payload.get('rep_id')}")
    print(f"  'amount' field present after migration: {'amount' in migrated_payload}")
    print(f"  'owner_id' field present after migration: {'owner_id' in migrated_payload}")

    # Validate migrated event against v2.0 schema
    valid_v2, errors_v2 = registry.validate(migrated_event, V2_0)
    print(f"  v2.0 validation of migrated event: {'PASS' if valid_v2 else 'FAIL'} (errors: {errors_v2})")

    # ------------------------------------------------------------------
    # Scenario D: Multi-version fanout — producer at v1.2, 4 consumer groups
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("Scenario D: Multi-version fanout (producer v1.2 → consumers v1.0, v1.1, v1.2, v2.0)")
    print("=" * 72)

    event_v1_2 = {
        "event_id": "evt-002",
        "event_type": EVENT_TYPE,
        "schema_version": "v1.2.0",
        "produced_at": "2026-04-13T11:00:00Z",
        "payload": {
            "opportunity_id": "OPP-2026-002",
            "account_id": "ACC-5522",
            "stage": "Negotiation",
            "amount": 280000.0,
            "close_date": "2026-08-15",
            "owner_id": "rep-david-m",
            "opportunity_score": 0.88,
            "region": "NORTHEAST",
        },
    }

    consumer_groups = [
        ConsumerVersionGroup(consumer_ids=["sap-consumer-1", "sap-consumer-2"], version=V1_0),
        ConsumerVersionGroup(consumer_ids=["analytics-consumer"], version=V1_1),
        ConsumerVersionGroup(consumer_ids=["crm-sync-consumer"], version=V1_2),
        ConsumerVersionGroup(consumer_ids=["new-platform-consumer"], version=V2_0),
    ]

    fanout = MultiVersionFanout(registry, migrator)
    results = fanout.fanout(event_v1_2, V1_2, consumer_groups)

    for version_str, result in results.items():
        print(f"\n  Consumer version {version_str} ({result['consumers']}):")
        print(f"    Delivery mode: {result['delivery_mode']}")
        if result["event"]:
            payload = result["event"].get("payload", result["event"])
            print(f"    Payload keys: {sorted(payload.keys())}")
            if "amount_usd" in payload:
                print(f"    amount_usd={payload['amount_usd']}, rep_id={payload.get('rep_id')}")
            else:
                print(f"    amount={payload.get('amount')}, owner_id={payload.get('owner_id')}")
        else:
            print(f"    Error: {result.get('error')}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("SCHEMA EVOLUTION DESIGN RULES")
    print("=" * 72)
    print("Backward compatible (minor version, no migration needed):")
    print("  - Adding optional fields with defaults (NEW_OPTIONAL_FIELD)")
    print("  - Making required fields optional (REQUIRED_TO_OPTIONAL)")
    print("  - Adding deprecation markers (FIELD_DEPRECATED)")
    print("\nBreaking changes (major version required + migration):")
    print("  - Renaming fields (FIELD_RENAMED)")
    print("  - Changing field types (TYPE_CHANGED)")
    print("  - Adding required fields (NEW_REQUIRED_FIELD)")
    print("  - Removing fields used by consumers (FIELD_REMOVED)")
    print("\nMulti-step migration: v1.0 → v1.1 → v1.2 → v2.0")
    print("  Each step is independently testable. No single migration")
    print("  from v1.0 → v2.0 required — compose the chain.")
    print("\nFanout delivery modes:")
    print("  exact_match: same version, no transformation")
    print("  forward_compatible: old consumer reads new event (ignores new optional fields)")
    print("  backward_compatible: new consumer reads old event (fills defaults)")
    print("  migrated: cross-major-version, EventMigrator applied")


if __name__ == "__main__":
    main()
