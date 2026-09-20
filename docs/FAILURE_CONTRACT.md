# Measure the SQLite failure contract

Use this experiment before adopting `SQLiteOutbox` for a small service that can
commit its business effect and deduplication record in the same SQLite database.
It tests that boundary with real child-process termination and shows why an
external API needs its own idempotency contract. It does not require a broker,
cloud account, additional framework, or any dependency beyond this package and
Python's standard library. All records are synthetic. HTTP traffic stays on
`127.0.0.1` using an automatically allocated port.

## Run it

From a checkout with the package installed (`python -m pip install -e .`):

```sh
python examples/failure_contract.py
```

The default uses 100 unique events and compares one and two worker processes.
JSON is printed to standard output; temporary databases are removed on exit.
Use a **new**, nonexistent directory to retain databases and raw latency samples:

```sh
python examples/failure_contract.py --directory /tmp/my-failure-contract
```

The opt-in larger workload includes eight competing processes:

```sh
python examples/failure_contract.py --events 10000 --workers 1 2 8 --timeout 600 \
  --directory /tmp/my-failure-contract-10000 > /tmp/my-failure-contract-10000.json
```

`--events` accepts 1–10,000, worker counts must be unique and between 1 and 8,
and `--timeout` accepts a positive deadline up to 1,800 seconds per subprocess
group. Each process-crash phase has its own deadline. On deadline failure the
parent kills and reaps its children. HTTP sockets have three-second timeouts,
and the HTTP server is stopped on completion or error. Interrupting the entire
process with an uncatchable kill is outside that cleanup guarantee. Use a local
disk and avoid running other benchmarks while collecting comparable timings.
Do not point this example at an application database.

## What a successful run proves

| Experiment | Injected condition | Required outcome |
|---|---|---|
| Crash before commit | Child inserts the business row, then exits with code 23 before its transaction commits | Zero committed effects after the crash; fresh-process replay creates one effect |
| Crash after commit | Child commits inbox and effect, then exits before marking the outbox published | One pending delivery; fresh-process replay creates no second effect and clears pending delivery |
| Concurrent duplicates | Each process submits every ID twice | Exactly one effect and inbox record per unique event, with no SQLite operational errors |
| HTTP negative control | Fake API commits the effect, then drops the first response; relay retries without a key | Client sees an unknown outcome, then acknowledgment; **two** remote effects |
| HTTP with downstream idempotency | Same lost response, with a stable key and transactional deduplication at the fake API | Two requests, **one** remote effect |
| Writer lock | Parent holds a write transaction while a child probes with zero timeout | Child must report SQLITE_BUSY/SQLITE_LOCKED before the parent releases the lock; helper consumption then succeeds |
| Pending backlog | Load the requested number of pending records | All records are returned; elapsed time and Python allocation peak are reported |

The effects table deliberately has no uniqueness constraint. A duplicate
business effect therefore cannot be hidden by a test-only database constraint.
The HTTP cases use a separate remote database. The local outbox cannot atomically
commit that remote effect or infer success from a lost response. The fake API's
key store has no expiry; a real API's retention period, payload matching rules,
concurrency behavior, and retry policy must be verified separately.

## Read the measurements

`benchmarks` contains direct SQL and `SQLiteOutbox.consume` results on the same
machine, with the same payloads, table schema, canonical JSON serialization,
connection-per-attempt behavior, and `BEGIN IMMEDIATE` transaction boundary.
The direct SQL reference implements payload-identity validation and atomic
inbox-plus-effect writes, rather than comparing the helper with an unprotected
insert. Startup and schema initialization are excluded from the timed region.
Both implementations run sequentially; process scheduling, caches, and run order
can still affect the results.

Each worker attempts every unique event twice. Consequently `attempts` is
`2 × events × workers`; `attempts_per_second` includes duplicate checks and is
**not** unique business effects per second. Latency p50/p95/p99 covers individual
consume attempts, including time waiting on database locks. Parent elapsed time
also includes result serialization and child shutdown. Raw `samples_ms` are
retained in each benchmark directory when `--directory` is supplied.

The output separates the imported runtime version from installed distribution metadata,
which can be stale during source development. It records SHA-256 hashes of the
executed harness, imported package initializer, and SQLiteOutbox source, plus the
harness checkout revision and dirty status. It exposes no local source paths.
A dirty checkout revision alone does not identify the executed bytes; retain
the hashes and matching source. These fields are provenance, not artifact
attestations or proof that all installed dependency versions match.

The output also records Python, SQLite, operating system, actual SQLite journal and
synchronous settings, database size, worker count, and workload size. Keep these
with any quoted results. The helper's connection timeout is currently 30 seconds;
the lock experiment confirms contention using a separate zero-timeout probe
while the parent still holds its transaction. Python 3.11+ records SQLite error
codes and names; Python 3.10 lacks those exception attributes, so the output
explicitly identifies the exact SQLite lock exception message as its evidence.
An unrelated OperationalError fails the experiment. It then measures successful helper
consumption after release, not exhaustion of the helper timeout
or recovery from arbitrary disk errors.

`backlog.python_peak_bytes` measures Python allocations during `pending()` with
`tracemalloc`, **not process RSS** or total SQLite memory. Fixture insertion uses
one bulk transaction and is excluded from this measurement. `pending()` currently
materializes the entire backlog, so test your own expected queue size before
choosing it. This experiment does not add pagination or retention.

A passing run establishes these synthetic safety counts on one host. It does
not establish a production throughput limit, an SLO, fairness between workers,
filesystem power-loss durability, multi-host coordination, or universal
exactly-once execution. Do not use it as a performance comparison with Temporal,
Inngest, or DBOS. Those systems have different execution and operational scopes.
If your workload cannot keep inbox and effect in one database, use a downstream
idempotency contract or an explicit reconciliation process for unknown outcomes.

## Regression checks

```sh
python -m pytest tests/test_failure_contract.py tests/test_sqlite_outbox.py -q
```

The regression suite uses eight events, asserts safety and cleanup, and does
not impose timing thresholds. It additionally checks that the SQL reference and
the helper both reject changed payloads for a reused ID, roll back a failed
effect, and refuse to overwrite an existing output directory. The 10,000-event
run is opt-in so ordinary CI does not depend on a particular runner's disk speed.
For a larger synthetic application using the same primitive, see
[the governed service demo](../examples/governed_service_demo.py).
