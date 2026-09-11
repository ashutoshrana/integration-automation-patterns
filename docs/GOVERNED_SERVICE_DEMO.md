# Governed service workflow: reproducible local integration

This example composes five libraries with a fake model, synthetic records, and an explicitly synthetic reviewer. It makes no provider calls, sends no telemetry over the network, and creates no real Salesforce or other external records. Business effects are rows in a local SQLite database.

## One-command bootstrap and run

With the five repositories checked out as siblings at the revisions below, run this single shell command from `integration-automation-patterns`:

```sh
python3 -m venv .venv-demo && .venv-demo/bin/python -m pip install -e '.[test]' -e ../enterprise-rag-patterns -e ../regulated-ai-governance -e ../confidence-escalation -e ../voice-ai-governance opentelemetry-sdk==1.44.0 && .venv-demo/bin/python examples/governed_service_demo.py
```

Dependency installation requires network access; the demo itself uses only local files and processes. Python 3.10+ is required by the integration library. Tested locally on Python 3.14.4; the dedicated CI job uses Python 3.12. To repeat after installation, run only `.venv-demo/bin/python examples/governed_service_demo.py`.

The command creates a fresh temporary directory and prints its path and JSON metrics. An optional `--directory /absolute/new-directory` keeps the artifacts in a chosen location. Use a fresh directory; existing workflow tables are not overwritten. Run `.venv-demo/bin/python -m pytest tests/test_governed_service_demo.py -q` for the regression check.

## Tested source revisions

The [dedicated CI workflow](../.github/workflows/governed-service-demo.yml) installs these exact dependency commits, independently of their published package versions:

| Dependency | Commit |
|---|---|
| enterprise-rag-patterns | `86899f0b67dcc5a1c959aae4d4cafd5b03db3992` |
| regulated-ai-governance | `2ec0a7a76a74a8162e70235bc530f07fe8d012e7` |
| confidence-escalation | `6a222914d047475079c4c4af43874b29cf1a18d3` |
| voice-ai-governance | `984df0dba2f3f9cb6a63fdc745f37198831a6a82` |

The integration package is the current checkout. OpenTelemetry API/SDK 1.44.0 uses semantic-conventions package 0.65b0. This demo uses general trace/span IDs, parent relationships, names, and durations; it does not claim conformance to evolving GenAI semantic attributes. No prompts, model responses, entity values, or approval text are included in spans. For future GenAI attribute mapping, use the [official development conventions](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/README.md) and pin the adopted schema. The local exporter is deliberately independent of that changing schema.

## What the scenarios prove

1. **Retrieval boundary:** `FERPAContextPolicy` filters private records before the fake model receives them. `model_input.json` records the actual model input, including one authorized fixture and excluding different-person, different-campus, and missing-metadata canaries. `retrieval_audit.log` records access.
2. **Audited action checks:** `GovernedActionGuard` requires an audit sink and emits decision and execution records to SQLite. An explicitly denied action never produces a business effect.
3. **Review and resume:** low confidence persists `pending_review` in SQLite. A fixture reviewer approves two requests. Separate resumed worker processes reload the current action policy; revoking one request after approval prevents its execution. The fixture reviewer is not a real identity/approval system and its updated confidence is a synthetic test input, not a calibrated estimate.
4. **Actual process crash:** the successful worker commits `SQLiteOutbox.consume`, then terminates with exit code 23 before `mark_published`. Its event remains pending. A new process replays it and produces no additional business effect. Deduplication and the effect share the same SQLite transaction; this does not make external network effects atomic.
5. **Handoff privacy:** `WarmTransferStateManager` produces a scrubbed handoff, including its summary. `handoff.json` contains the output; the configured email fixture must be absent.
6. **Shared trace:** W3C trace context follows the subprocess boundary. `spans.jsonl` contains a single shared trace, with local operation spans and elapsed durations. The intentionally killed process can leave its outer span unfinished; a committed child span and SQLite state preserve the observable crash boundary.

The executable asserts zero observed unauthorized disclosures/actions, duplicate effects, and fixture redaction failures. Expected business effects are exactly `success` and `approved`; `revoked` and `denied` finish denied. Expected escalation classification is 4/4 for these four hand-constructed cases. The JSON reports counts, classifications, persisted state, trace count, and total elapsed milliseconds including process startup. These are bounded local regression results, not production reliability, regulatory compliance, or calibration estimates.

## Review limits

No real LLM, identity provider, human approval UI, enterprise database, or network telemetry backend is exercised. The trusted local fixture database carries authorization state; this example does not solve simultaneous external revocation during an in-flight action. Mandatory audit delivery before execution is enforced, but a process crash can interrupt the subsequent outcome audit; consumers must reconcile durable effects and audit records. SQLite serialization and same-database effects are intentional limits. The normal dependency-light test suite skips this optional integration test when sibling dependencies are absent; the dedicated CI installs them and runs it explicitly.
