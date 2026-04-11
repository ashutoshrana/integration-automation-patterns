# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-04-11

### Added

**Core modules:**
- `event_envelope.py` — reliable event transport for enterprise integration:
  - `EventEnvelope` — idempotency key, delivery status lifecycle, structured audit logging
  - `DeliveryStatus` — PENDING → DISPATCHED → ACKNOWLEDGED → PROCESSED / RETRYING / FAILED / SKIPPED
  - `RetryPolicy` — bounded retry with fixed or exponential backoff and configurable cap
- `sync_boundary.py` — system-of-record synchronization contracts:
  - `SyncBoundary` — explicit field-level authority assignment between two systems
  - `RecordAuthority` — SYSTEM_A / SYSTEM_B / SHARED / MANUAL ownership model
  - `SyncConflict` — structured representation of detected bi-directional update conflicts

**Documentation:**
- `docs/architecture.md` — layered integration architecture (source → integration → workflow → SOR → observability)
- `docs/implementation-note-01.md` — event-driven integration reliability patterns

**Project infrastructure:**
- `LICENSE` — MIT
- `CITATION.cff` — enables GitHub "Cite this repository" button
- `CONTRIBUTING.md` — contribution guidance
- `GOVERNANCE.md` — project governance model
- `ROADMAP.md` — near-term development direction
- `pyproject.toml` — full build configuration with keywords, classifiers, and optional dependency groups
- GitHub Actions CI: pytest (Python 3.10–3.12), ruff lint, mypy type check
- Issue templates: bug report, feature request
- 42 passing tests covering all public module APIs
