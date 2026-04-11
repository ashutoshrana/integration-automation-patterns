# ADR 002 — Explicit field-level authority, not record-level ownership

**Status:** Accepted  
**Date:** 2026-04-11  
**Context:** integration-automation-patterns / sync_boundary module

---

## Context

Enterprise integration commonly involves bi-directional synchronization between
two systems that both hold overlapping data — for example, a CRM and an ERP
that both store account records, contact information, or financial data.

When the same field exists in both systems and either system can update it,
the integration layer must define an authority model: which system's value
wins when there is a conflict?

Two approaches are common:

1. **Record-level ownership:** One system owns the entire record. All fields
   in that record are authoritative in the owning system. The other system
   receives updates but cannot initiate changes.
2. **Field-level authority:** Authority is defined per field. System A may be
   authoritative for account name and billing address; System B may be
   authoritative for contract value and renewal date; some fields may be
   shared (either system can update, both are notified).

---

## Decision

`SyncBoundary` implements **field-level authority** with four authority values:

- `RecordAuthority.SYSTEM_A` — this field is authoritative in System A
- `RecordAuthority.SYSTEM_B` — this field is authoritative in System B  
- `RecordAuthority.SHARED` — either system may update; conflict requires
  resolution logic
- `RecordAuthority.MANUAL` — authority cannot be determined programmatically;
  requires human review

Field authority is declared explicitly in the `SyncBoundary` constructor as a
dict mapping field name to `RecordAuthority`. There is no implicit default:
unmapped fields are treated as outside the sync scope.

---

## Rationale

### Field-level authority reflects real integration requirements

In production enterprise integrations, record-level ownership is rarely
sufficient:

- A CRM account record and an ERP customer record overlap on company name,
  address, and industry classification — but the CRM owns the sales contact
  history and the ERP owns the billing and payment terms. Neither system owns
  the full record.
- A student information system and an LMS share enrollment status and course
  registration — but the SIS is authoritative for enrollment decisions and
  the LMS is authoritative for progress and completion records.
- An HR system and a facilities system share employee location data — but
  HR owns the employment relationship and facilities owns the office assignment.

Attempting to assign record-level ownership to any of these relationships
creates artificial constraints that break down as soon as one team modifies
a field "owned" by the other system.

### Explicit field mapping: no hidden assumptions

`SyncBoundary` requires explicit field authority assignment. The absence of
a field from the authority map means it is not managed by this sync boundary.
This is intentional:

- Unmapped fields are not silently assumed to belong to either system.
- Adding a new synchronized field requires an explicit decision about its
  authority — it cannot be inherited implicitly from a record-level policy.
- The authority map is inspectable: `fields_owned_by(authority)` returns the
  set of fields under a given authority, making the sync contract auditable.

### MANUAL authority: explicit acknowledgment of human-in-loop cases

In practice, some fields cannot have their authority determined programmatically.
Examples: fields that represent negotiated agreement between two teams, fields
where the source of truth is a legal document rather than either system,
fields where the authority changes based on workflow state.

`RecordAuthority.MANUAL` encodes these cases explicitly rather than forcing
a binary SYSTEM_A/SYSTEM_B assignment that would be wrong. The `SyncConflict`
object surfaces MANUAL fields for human review.

### SHARED authority and conflict detection

`SHARED` fields acknowledge that some data is legitimately updateable from
either side. `SyncBoundary.detect_conflict()` identifies SHARED fields where
both systems show changes since the last sync — these are the cases that require
resolution logic (last-write-wins, merge, escalation, etc.).

The module does not implement a conflict resolution strategy: that decision
is domain-specific and belongs in the calling integration layer. The
`SyncConflict` object provides the structured representation needed to
implement any resolution strategy.

---

## Consequences

### Accepted trade-offs

- All synchronized fields must be explicitly mapped. This is more upfront work
  than a record-level ownership model, but this cost is paid once per
  integration point and prevents ambiguity.
- `detect_conflict()` requires a `last_sync` snapshot of field values to
  determine which system changed a field since the last sync. If `last_sync`
  is not available (first sync, snapshot lost), all differing field values are
  reported as potential conflicts. Callers should treat first-sync conflicts
  as requiring manual verification.
- `RecordAuthority.MANUAL` fields are surfaced in conflicts but are not
  resolved by the module. The integration layer must route them appropriately
  (e.g., to a human review queue or a workflow escalation).

### Alternatives considered

**Record-level ownership:** Simpler to reason about for a single integration
pair. Does not scale when multiple teams own different subsets of the same
record, or when authority changes over the record lifecycle. Rejected for
general-purpose use.

**Implicit default authority (all unmapped fields go to SYSTEM_A):** Hides
fields that have no explicit assignment. Creates subtle integration bugs when
a new field is added to one system and its authority is assumed rather than
declared. Rejected.

---

## Related

- `sync_boundary.py` — `SyncBoundary`, `RecordAuthority`, `SyncConflict`
- ADR 001 — Idempotency key design
- `docs/implementation-note-01.md` — Event-driven integration reliability patterns
