# ADR-0001: Separate Integration Transport from Workflow State

## Status

Accepted

## Decision

Transport concerns such as webhook handling, message delivery, and retries should remain distinct from workflow state transitions.

## Why

Mixing transport behavior with workflow state creates replay errors, duplicate actions, and weak recovery paths.
