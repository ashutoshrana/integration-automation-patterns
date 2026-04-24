# Adoption

## PyPI Downloads

Verified via [pypistats.org](https://pypistats.org/packages/integration-automation-patterns) — independent third-party statistics.

| Week of | Downloads |
|---------|-----------|
| 2026-04-13 | ~2,374 |
| 2026-04-20 | ~2,079 |

Downloads are organic — no self-installs, no promotional campaigns.

## How It Is Used

`integration-automation-patterns` provides structural solutions to the recurring failure modes of enterprise integration:

| Problem | Pattern | Use Case |
|---------|---------|----------|
| Duplicate event processing | Idempotent Event Envelope | Kafka consumers, webhook handlers |
| Partial transaction failures | Saga Orchestrator | Multi-system CRM + ERP sync |
| Silent data conflicts | Sync Boundary | Salesforce ↔ Oracle bidirectional sync |
| Unrecoverable workflow state | Transactional Outbox | Guaranteed event delivery |
| Agent tool call authorization | MCP Security Patterns | Regulated AI agent pipelines |

## Production Architecture Patterns

These patterns are extracted from production systems operating at enterprise scale:

- **8-flow state machine** for voice AI lead qualification (Twilio + Salesforce + Oracle)
- **Multi-system saga orchestration** across CRM, ERP, and marketing automation
- **Transactional outbox** for guaranteed event delivery in cloud-native integration
- **Idempotent webhook processing** for Salesforce, Oracle, and third-party API consumers

## 43 Integration Examples

Covering event-driven architecture, workflow orchestration, API gateway patterns, distributed caching, service resilience, CQRS, event sourcing, and MCP security — all with runnable Python implementations and full test coverage.

## Related Packages

- [enterprise-rag-patterns](https://pypi.org/project/enterprise-rag-patterns/) — Compliance-first RAG retrieval patterns
- [regulated-ai-governance](https://pypi.org/project/regulated-ai-governance/) — AI agent governance across 25 jurisdictions
- [ferpa-haystack](https://pypi.org/project/ferpa-haystack/) — Haystack-native FERPA document filter
