# Implementation Note 08 — Schema Evolution in Enterprise Integration: How to Change Event Schemas Without Breaking Consumers

**Repo:** `integration-automation-patterns`
**Relates to:** [`examples/10_schema_evolution.py`](../examples/10_schema_evolution.py)
**ADR cross-reference:** [ADR-001 (idempotency key design)](adr/001-idempotency-key-design.md), [ADR-003 (orchestration vs. integration boundaries)](adr/003-orchestration-vs-integration-boundaries.md)

---

## The problem that looks obvious until it happens

A producer service adds a new field to its Salesforce opportunity event. The field is optional. All existing consumers will still receive the event and process it normally — the new field is just ignored. No coordination required, no migration, no downtime.

This works correctly in staging, where all services are deployed at the same time. It fails in production, where four consumer services are deployed independently over a two-week sprint. By the time the last consumer deploys, the producer has shipped three more changes. Two of those changes were backward compatible. One was not: the team renamed a field they had initially called `amount` to `amount_usd` to clarify the currency. The rename was approved in the team's ADR.

Two consumers still at v1.x receive events with `amount_usd`. They silently drop the value (no field named `amount` found) and write `null` or `0.0` to SAP Finance. No error is raised. No alert fires. The issue is discovered six weeks later in a quarterly finance reconciliation.

This is not a testing failure. It is a schema evolution governance failure.

---

## The four evolution rules that matter

Every field change falls into one of four categories:

**Fully compatible (no major version bump, no migration):**

1. `NEW_OPTIONAL_FIELD` — Adding a field with a default value. Old consumers ignore it; new consumers see it with the producer's value or the schema default. Requires: the field must have a sensible default, and old consumers must not fail on unexpected fields (most serialization libraries handle this correctly, but validate it).

2. `REQUIRED_TO_OPTIONAL` — Making a previously required field optional. Old producers still emit it; old consumers still receive it. New producers may omit it; new consumers fill in the default. Net effect: zero breakage in either direction.

**Forward-compatible only (older consumers can read new events, but not vice versa):**

3. `FIELD_REMOVED` — Removing a field. New events no longer contain it. Old consumers that expect it receive null or their deserializer default. Safe if old consumers have null handling; dangerous if they do not.

**Breaking (requires major version bump + migration):**

4. `FIELD_RENAMED` — The most common source of silent data corruption. A renamed field looks like a removed field to old consumers (they look for the old name, find nothing, get null) and a new field to consumers that were never told about the rename. Old consumers writing null to a financial amount field is a real incident.

5. `TYPE_CHANGED` — Changing a string to a float (or vice versa) breaks consumers that have already deserialized the field as the old type. Even if the values are numerically equivalent (`"125000"` vs. `125000.0`), type coercion behavior is serialization-library-specific and should not be relied upon across services.

6. `NEW_REQUIRED_FIELD` — Old producers cannot emit the new required field; their events fail validation at the new consumer. Requires a migration period where the field is first introduced as optional, then made required after all producers have deployed.

---

## The major/minor version contract

The semantic version contract for event schemas:

- **Minor version increment** (v1.0 → v1.1): backward AND forward compatible. Only `NEW_OPTIONAL_FIELD`, `REQUIRED_TO_OPTIONAL`, and `FIELD_DEPRECATED` changes allowed. Any consumer at any v1.x version can read any event produced at any v1.x version.

- **Major version increment** (v1.x → v2.0): breaking changes allowed. Cross-major-version reads require an explicit `EventMigrator` mapping. No consumer should receive v2.0 events without either deploying to v2.0 or being served a migrated event via `MultiVersionFanout`.

This is the contract that prevents the `amount` → `amount_usd` incident: renaming a field requires a major version bump, which forces the migration mapping to be written before the change ships.

---

## The migration chain: step-by-step, not leap-of-faith

When a v1.0 consumer needs to read v2.0 events, the migration path is:

```
v1.0 → v1.1 → v1.2 → v2.0
```

Not:

```
v1.0 → v2.0 (one big migration)
```

The step-by-step chain has several advantages:

1. **Each step is independently testable.** The v1.1 → v1.2 migration (add region default) is trivially testable. The v1.2 → v2.0 migration (rename amount, rename owner_id) is independently testable. A combined v1.0 → v2.0 migration that does all four transformations simultaneously is harder to verify.

2. **Intermediate versions can be used.** Consumers that are at v1.2 need only the v1.2 → v2.0 migration, not the full chain.

3. **Rollback is cleaner.** If v2.0 is rolled back, the intermediate migration steps are still valid.

The `EventMigrator` uses BFS to find the shortest migration path from source to target, so callers need not know the intermediate steps.

---

## Multi-version fanout: the operational reality

In an enterprise with many consumers, you will never have all consumers at the same schema version simultaneously. This is not a failure of planning — it is the expected state of a system where teams deploy independently.

The `MultiVersionFanout` pattern accepts this reality and handles it structurally:

1. The producer emits events at its current version.
2. The fanout broker (or the producer's outbox) maintains a `ConsumerVersionGroup` registry.
3. For each consumer group, the fanout resolves the compatibility mode and applies the appropriate transformation:
   - Same version: direct delivery.
   - Compatible minor versions: direct delivery with schema defaults filled in.
   - Cross-major versions: EventMigrator applied before delivery.
4. Each consumer receives an event that validates against its registered schema.

The key constraint: **the consumer version group registry must be updated before the producer ships a breaking change.** If a consumer at v1.x is not registered, it will receive v2.0 events unmigrated and fail.

In practice, the version group registry is updated in the same release as the migration rule registration. The release process:

1. Register the new schema version in `SchemaRegistry`.
2. Register the migration rule in `EventMigrator`.
3. Update the consumer version group registry to include any consumers that need migration.
4. Deploy. Producers can now emit the new version; all registered consumers receive correctly migrated events.
5. Consumers can deploy to the new version at their own pace. When a consumer deploys to v2.0, remove it from the migration path — it no longer needs migration.

---

## Schema validation: the gate that prevents silent corruption

Every event should be validated against its declared schema version before it is written to the outbox or event broker. The `SchemaRegistry.validate()` call is not optional.

The failure mode for skipping validation: a producer is updated to emit v2.0 events but does not update its `schema_version` field. Events declare `v1.0` but contain v2.0 field names. The fanout resolves compatibility as "exact_match" (both say v1.0) and delivers unmigrated v2.0 events to v1.0 consumers. Consumers receive `amount_usd` and find no `amount` field.

Validation catches this before the event leaves the producer: the declared v1.0 schema requires `amount`; the event contains `amount_usd`; validation fails at the source.

---

## What to do when a migration path does not exist

Not all version gaps can be migrated. If a consumer is at v0.8 and the current schema is v2.0, but the migration chain only goes back to v1.0, `EventMigrator.can_migrate()` returns False.

Options, in order of preference:

1. **Extend the migration chain.** Add v0.8 → v1.0 migration rules. This is usually the right answer if the consumer cannot be updated.

2. **Force a consumer upgrade.** If the migration would require too many transformation rules with too many assumptions, it is safer to require the consumer to update to a supported version.

3. **Dead-letter with a structured error.** If neither option is viable immediately, route to dead-letter with `delivery_mode=migration_failed` and the specific version gap noted. Do not silently deliver a partially-migrated event.

The worst outcome is option 4: silently deliver the unmigrated event and let the consumer handle a deserialization error at runtime. This pushes the schema evolution error into the application error path where it is harder to diagnose.

---

## What this pattern does not cover

- **Schema Registry as a service:** The `SchemaRegistry` in this example is in-memory. Production systems use a durable schema registry (Confluent Schema Registry, AWS Glue Schema Registry, Apicurio) that persists schema versions across service restarts and provides a central API for consumers to fetch their schemas.
- **Avro/Protobuf/JSON Schema encoding:** This example uses Python dicts. Production schema evolution is typically implemented with Avro (for Kafka) or Protobuf. The `FieldEvolutionRule` classification system maps directly to Avro's schema evolution rules.
- **Schema compatibility enforcement in CI:** The most important operational control is blocking schema changes that violate the minor/major version contract in CI. The `FieldEvolutionRule.TYPE_CHANGED` check can be run as a schema diff step in the producer's CI pipeline before the change ships.
- **Consumer-Driven Contract Testing (CDCT):** Pact and similar frameworks extend this pattern by having consumers publish their schema expectations, and producers verify their events against those contracts in CI.
