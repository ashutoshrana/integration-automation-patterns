# ADR-002: Explicit Field-Level Authority Model for Bi-Directional Sync

**Status:** Accepted  
**Date:** 2026-04-12  
**Deciders:** Ashutosh Rana

## Context

Enterprise integration commonly involves bi-directional synchronization between systems that both hold overlapping data — a CRM and ERP both storing account records, a student information system (SIS) and LMS sharing enrollment data, or an HR system and a facilities system sharing employee location. When both systems can update the same fields, the integration layer must answer: when a conflict occurs, which system's value wins?

Two approaches were evaluated:

1. **Record-level ownership:** Designate one system as the "source of truth" for the entire record. All fields in that record belong to the owning system. The other system receives updates but cannot initiate changes.
2. **Explicit field-level authority:** Authority is declared per field. Different fields on the same record may be authoritative in different systems.

## Decision

`SyncBoundary` implements explicit field-level authority. Authority is one of `SYSTEM_A`, `SYSTEM_B`, `SHARED` (either system may update; conflicts require resolution), or `MANUAL` (authority is human-determined). Unmapped fields are outside the sync scope — no implicit defaults.

## Rationale

**Record-level ownership breaks on real data structures:**

In practice, no single record belongs entirely to one system. A CRM account and ERP customer record overlap on name, address, and industry — but the CRM owns sales contact history and the ERP owns billing terms. A SIS enrollment record and LMS enrollment record overlap on status and course ID — but the SIS owns enrollment decisions and the LMS owns completion data. Forcing record-level ownership onto these structures requires either splitting records artificially or accepting that one system will regularly overwrite fields it doesn't legitimately control.

**Explicit mapping surfaces authority decisions that must be made anyway:**

The integration team must decide field authority regardless of the model used. Record-level ownership hides this decision inside an implicit rule that breaks when exceptions occur. Explicit field mapping forces the decision to be documented at integration design time, where it can be reviewed and version-controlled. When a conflict occurs in production, the `SyncBoundary` authority map is the authoritative reference — there is no ambiguity about which system should win.

**`SHARED` and `MANUAL` encode real scenarios, not theoretical edge cases:**

Fields like a customer's primary phone number are legitimately updateable by both CRM (sales reps) and ERP (billing team). Forcing these into `SYSTEM_A` or `SYSTEM_B` ownership means the non-owning system's updates are silently discarded. `SHARED` authority with conflict detection is the honest representation: both systems may update, and when they both change the value between syncs, a resolution step is required. `MANUAL` authority acknowledges that some fields have authority determined by business rules or negotiated agreements that cannot be encoded in a sync configuration.

## Consequences

**Positive:**
- Conflicts are surfaced as structured `SyncConflict` objects with field-by-field authority data, enabling precise resolution strategies (last-write-wins, escalation, domain-specific merge logic) rather than blanket overwrites.
- The authority map is inspectable and auditable: `fields_owned_by(SYSTEM_A)` returns the exact set of fields under System A's authority, which can be logged and reviewed for compliance.
- New synchronized fields require an explicit authority declaration — they cannot be silently inherited from a record-level default, preventing accidental overwrite of authoritative data.

**Negative:**
- Requires upfront field-by-field authority analysis for each sync boundary. For records with 30–50 fields, this is non-trivial design work. Record-level ownership gets an integration running faster even if it creates correctness problems later.
- `detect_conflict()` requires a `last_sync` snapshot. Without it, all differing values are reported as potential conflicts, requiring either manual verification or an initial-sync strategy.

**Neutral:**
- The library provides the conflict detection and representation machinery but not the conflict resolution strategy. Resolution logic is domain-specific (last-write-wins, field-specific merge, human escalation) and belongs in the calling integration layer.
