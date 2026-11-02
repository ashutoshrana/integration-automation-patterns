# Implementation Note 01: Retry-Safe Event Handling and Action Logging

## Problem

Enterprise integration flows often fail in a predictable way: an event is received twice, a downstream update times out, and the automation layer can no longer say with confidence what actually happened.

This usually comes from weak boundaries between:
- event transport
- workflow progression
- system-of-record updates
- operational logging

## Pattern

Treat every inbound event as something that may be replayed.

The implementation pattern is:
1. validate the payload
2. derive or read a stable external event identifier
3. atomically claim that identifier and apply local business changes in one transaction
4. use the same identifier for remote effects whose outcome may be uncertain
5. log the action and outcome in a way that can be inspected later

## Why it matters

This improves:
- duplicate-event safety
- operational recovery
- audit clarity
- trust in downstream case or task state

## Minimum logging model

For every integration-side action, capture:
- event identifier
- mapped target object
- timestamp
- action attempted
- result
- retry or replay status

## Failure mode to avoid

Do not allow transport retries to silently trigger business-state transitions a second time. If transport and workflow logic are coupled too closely, replay becomes indistinguishable from legitimate progress.

## Practical takeaway

An in-memory check followed by a write does not close the concurrent-delivery or crash window. A database uniqueness constraint must be committed with the business effect; remote services need their own idempotency contract.

Reliable enterprise integration is less about moving data and more about preserving confidence in what happened, what changed, and what can safely be retried.
