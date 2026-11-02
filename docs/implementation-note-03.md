# Implementation Note 03 — Sync Boundaries in CRM-ERP Integration

Example scope: snippets below include application wiring and internal excerpts from
[`src/integration_automation_patterns/sync_boundary.py`](../src/integration_automation_patterns/sync_boundary.py); undefined names are supplied by that example or the application.
They are not standalone package APIs. See the linked module for complete setup.


**Series:** integration-automation-patterns  
**Topic:** Why explicit field authority prevents data corruption in bi-directional sync  
**Status:** Published 2026-04-12

---

## The problem

Enterprises typically have two systems that need to agree on the state of
shared records: a CRM (Salesforce, HubSpot, Dynamics) and an ERP (SAP,
Oracle, NetSuite). Both systems have users making changes. Both systems have
automation making changes. Both systems assume they are the source of truth
for the records they manage.

The standard integration approach is to sync changes between systems using
a middleware layer (MuleSoft, Boomi, Azure Integration Services, custom code).
The integration watches for changes in each system and propagates them to the
other.

This works until both systems update the same field between sync cycles. The
integration then has a conflict: two different values for the same field, both
changed since the last sync, with no clear winner.

The common failure modes:

- **Silent overwrite:** The integration applies the value from whichever system
  was checked last. The other system's change is silently discarded. The user
  who made that change never knows. Data corruption accumulates over time.

- **Duplicate update loop:** System A writes to system B. System B's change
  trigger fires and writes back to system A. System A's change trigger fires.
  The systems ping-pong the update indefinitely until a circuit breaker stops it.

- **Field sprawl:** Over time, the integration accumulates conditional logic:
  "if this field changed and it came from system A and the record type is X and
  the account tier is Y, then apply it to system B." The logic becomes
  unmaintainable and no one can answer "which system is authoritative for this
  field?"

---

## Why authority must be explicit and structural

The root cause is that field authority is implicit. Everyone on the integration
team knows that "finance owns billing address" and "sales owns the segment," but
this knowledge lives in heads, Confluence pages, or comments in sync scripts.
It is not encoded in a form that is testable or inspectable at runtime.

When a new developer joins and writes a new sync flow, they may not know that
SAP is authoritative for payment terms. The field gets synced in the wrong
direction. Finance notices three months later that all their negotiated payment
terms have been overwritten by stale CRM data.

Making authority explicit — in a data structure, not documentation — solves
this at the source:

```python
from integration_automation_patterns import RecordAuthority, SyncBoundary

ACCOUNT_BOUNDARY = SyncBoundary(
    record_type="account",
    system_a_id="salesforce",
    system_b_id="sap_s4",
    field_authority={
        "primary_email":     RecordAuthority.SYSTEM_A,   # SF owns
        "crm_segment":       RecordAuthority.SYSTEM_A,   # SF owns
        "payment_terms":     RecordAuthority.SYSTEM_B,   # SAP owns
        "credit_limit_usd":  RecordAuthority.SYSTEM_B,   # SAP owns
        "legal_entity_name": RecordAuthority.SHARED,     # manual review
    },
    excluded_fields={"sf_lead_score", "sap_cost_center"},
)
```

This is the contract. It is code-reviewable and version-controlled. `detect_conflict()` reports differences;
the caller must enforce write direction, select a winner and persist review work.

---

## Conflict detection: last-sync-value, not timestamps

The naive conflict detection approach compares the current values in each
system:

```
if SF.payment_terms != SAP.payment_terms:
    # conflict?
```

This is wrong. A difference between the two systems' current values is not
necessarily a conflict. It is the normal state whenever one system has updated
a field and the sync hasn't run yet. If only one system changed the value since
the last sync, there is no conflict — there is just a pending propagation.

A conflict exists when **both systems changed the field independently** since
the last sync:

```
last_sync_value = "NET30"
SF.payment_terms  = "NET30"   # unchanged
SAP.payment_terms = "NET45"   # SAP updated — clean propagation, no conflict

last_sync_value = "NET30"
SF.payment_terms  = "NET60"   # SF updated
SAP.payment_terms = "NET45"   # SAP also updated — TRUE CONFLICT
```

`SyncBoundary.detect_conflict()` implements this correctly:

```python
# Internal logic excerpt (inside detect_conflict):
if last_sync_value is not None:
    a_changed = value_a != last_sync_value
    b_changed = value_b != last_sync_value
    if not (a_changed and b_changed):
        return None  # only one system changed — not a conflict
```

When `last_sync_value=None`, the API cannot distinguish an unknown baseline from a
previous null value and reports any unequal synced values as a conflict.

This requires storing the last-sync-value per field. That is additional state
to manage, but it prevents false conflicts (which cause unnecessary escalations)
and false non-conflicts (which cause silent overwrites).

---

## Three resolution strategies and when to use each

### 1. Authority-wins

The system that owns the field always wins. If SAP owns `payment_terms` and
both systems changed it, SAP's value is applied and SF's change is discarded.

Use when:
- The field has a single clear owner (most fields should be in this category)
- The owning system's users understand that their value is canonical
- Losing a non-owning-system change is acceptable and expected

This is a caller-side resolution policy for fields with `SYSTEM_A` or `SYSTEM_B`
authority; `SyncBoundary` does not execute it.

### 2. Last-writer-wins

The system that made the most recent modification wins, based on field-level
modification timestamps.

Use when:
- Neither system has a strong ownership claim
- Both systems are valid writers and the most recent state is the desired state
- Field-level timestamps are available from both systems

Limitation: requires reliable, comparable modification timestamps. Many systems
report record-level timestamps, not field-level timestamps. If the timestamps
are unavailable or unreliable, this strategy cannot be applied safely.

### 3. Manual review

The conflict is not resolved automatically. It is queued for a human data
steward to review and decide. The field is not synced until the review is
resolved.

Use when:
- The field is legally binding (legal entity name, VAT registration)
- Automated resolution would carry compliance or financial risk
- The cost of getting it wrong exceeds the cost of human review

Manual review is appropriate for `SHARED` fields. Applying an incorrect legal
entity name automatically — even with last-writer-wins — risks propagating an
error that requires legal correction to fix.

---

## Excluding system-internal fields

Every enterprise system has fields that are internal to that system and should
never be synced:

- CRM lead scores, opportunity probability, campaign attribution
- ERP cost centers, dunning levels, G/L account codes
- Internal notes, admin flags, system-generated identifiers

These fields must be **explicitly excluded** from the sync boundary, not just
omitted from the field authority map. The distinction matters:

- An **omitted field** is undefined — the sync engine may not know what to do
  with it, and a developer adding sync logic for a new field might accidentally
  pick it up.
- An **excluded field** is an explicit declaration that this field is never
  synced. Any code path that would try to sync it is an error.

```python
excluded_fields={"sf_lead_score", "sap_cost_center", "sf_internal_notes"},
```

`SyncBoundary.is_synced()` returns `False` for excluded fields.
`SyncBoundary.authority_for()` returns `None` for excluded fields.
Both make the "do not sync this field" intent testable.

---

## The allow_system_b_override flag

A caller implementing authority-wins might overwrite a non-owner's edit on its next
sync. If the application needs visibility into discarded edits, it must explicitly
record them and route them for review.

`allow_system_b_override` records configuration intent. `detect_conflict()` does not
read this flag, queue a review, resolve the conflict or apply a write. The integrating
application must implement those behaviors and its review queue explicitly.

An implemented review path can help during early rollout when data stewardship
boundaries are still being established. The flag alone does not surface writes.

---

## See also

- `examples/04_crm_erp_sync_boundary.py` — runnable demo of this pattern
- `src/integration_automation_patterns/sync_boundary.py` — `SyncBoundary`, `SyncConflict`, `RecordAuthority`
- ADR 002 — Explicit field authority model rationale
- Implementation Note 02 — Idempotency in event processing
