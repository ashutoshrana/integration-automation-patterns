# Ecosystem Coverage

`integration-automation-patterns` is the **enterprise messaging and workflow** layer of the
[Open Regulated AI Trilogy](https://github.com/ashutoshrana):

| Repo | Domain |
|------|--------|
| [enterprise-rag-patterns](https://github.com/ashutoshrana/enterprise-rag-patterns) | FERPA/GDPR-compliant RAG retrieval |
| **integration-automation-patterns** ← you are here | Enterprise integration, messaging, and workflow |
| [regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) | Cross-regulation AI policy enforcement |

---

## Message Brokers

| Broker | Pattern | Status |
|--------|---------|--------|
| Apache Kafka | `KafkaEventEnvelope` — partition key, schema version, DLQ routing | ✅ Implemented |
| RabbitMQ | AMQP envelope adapter | 🔲 Planned |
| AWS SQS / SNS | SQS envelope + SNS fan-out pattern | 🔲 Planned |
| Azure Service Bus | Session-based ordering envelope | 🔲 Planned |
| Google Pub/Sub | Push/pull envelope adapter | 🔲 Planned |
| Redis Streams | Lightweight stream envelope | 🔲 Planned |

---

## Webhook Sources

| Source | Signature Format | Status |
|--------|-----------------|--------|
| GitHub | `X-Hub-Signature-256: sha256=<hex>` | ✅ Implemented |
| Stripe | `Stripe-Signature: t=<ts>,v1=<hex>` | ✅ Implemented |
| Salesforce | `X-Salesforce-Signature: sha256=<hex>` | ✅ Implemented |
| ServiceNow | `X-ServiceNow-Signature: sha256=<hex>` | ✅ Implemented |
| HubSpot | `X-HubSpot-Signature-v3: sha256=<hex>` | 🔲 Planned |
| Twilio | `X-Twilio-Signature` (HMAC-SHA1) | 🔲 Planned |

---

## CDC Connectors

| Connector | Format | Status |
|-----------|--------|--------|
| Debezium | Standard envelope (`op`, `before`, `after`, `source`) | ✅ Implemented |
| AWS DMS | DMS change record format | 🔲 Planned |
| GCP Datastream | Datastream change record format | 🔲 Planned |
| Azure Data Factory | ADF change feed format | 🔲 Planned |
| Fivetran | Fivetran log format | 🔲 Planned |

---

## Resilience Patterns

| Pattern | Implementation | Status |
|---------|---------------|--------|
| Circuit Breaker | `CircuitBreaker` — CLOSED/OPEN/HALF_OPEN | ✅ Implemented |
| Retry with Backoff | `RetryPolicy` — fixed/exponential backoff | ✅ Implemented |
| Transactional Outbox | `OutboxProcessor` — broker-agnostic relay | ✅ Implemented |
| Saga Orchestrator | `SagaOrchestrator` — forward + compensation | ✅ Implemented |
| Bulkhead | Thread pool isolation | 🔲 Planned |
| Rate Limiter | Token bucket / sliding window | 🔲 Planned |
| Timeout | Callable deadline enforcement | 🔲 Planned |

---

## CRM / ERP Source Systems

| System | Sync Pattern | Status |
|--------|-------------|--------|
| Salesforce | `SyncBoundary` field-level authority | ✅ Core pattern |
| SAP | Field authority + conflict detection | ✅ Core pattern |
| ServiceNow | Event envelope + delivery tracking | ✅ Core pattern |
| HubSpot | Outbox + CDC adapter | 🔲 Planned |
| Microsoft Dynamics 365 | Change feed + sync boundary | 🔲 Planned |
| NetSuite | Event envelope adapter | 🔲 Planned |

---

## AI Framework Integration

These patterns are designed to be called from AI-driven workflows:

| Framework | Integration Point | Status |
|-----------|------------------|--------|
| LangChain | Tool / callback wrapping via `event_envelope` | 🔲 Planned |
| CrewAI | Task action with outbox pattern | 🔲 Planned |
| AutoGen | ConversableAgent with circuit breaker | 🔲 Planned |
| Semantic Kernel | KernelFunction wrapper | 🔲 Planned |
| LlamaIndex | Query pipeline integration | 🔲 Planned |
| Haystack | Pipeline component wrapper | 🔲 Planned |

See [regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) for AI agent policy enforcement.

---

## Contributing

To add coverage for a new message broker, CDC connector, or webhook source:

1. Open an issue using the appropriate issue template
2. Implement the adapter following patterns in existing modules
3. Add tests following the existing test style
4. Update this file and `CHANGELOG.md`

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the full contribution guide.
