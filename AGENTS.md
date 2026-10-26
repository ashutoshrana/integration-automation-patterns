# Repository validation notes

## Common Failure Patterns

| Symptom | Root cause | Fix |
|---|---|---|
| Duplicate broker delivery after publisher restart | Publish and mark-published are separate operations | Describe at-least-once delivery and commit consumer inbox ID with its business effect in one SQLite transaction; test crash and concurrent replay |
| MCP checks appear to authorize users without verifying identity | Illustrative metadata/hash checks are not protocol authentication | Use SDK auth middleware with trusted signed-token issuer/audience verification and per-subject grants; validate through HTTP tool calls |
| Permission changes preserve a manifest digest | Legacy digest excluded permissions/source and used shallow string sorting | Hash canonical JSON containing nested schema, source and permissions; require independent reapproval |
| MCP enqueue succeeds without audit records | Async sink calls returned unawaited coroutines | Reject async sinks and awaitable results; report event_committed on post-enqueue acknowledgment failure |
| Release uploads can drift from tested package identity | Release publication previously rebuilt without tag/source/runtime/artifact checks | Check out the release event commit, require matching stable tag and package versions, rerun tests and validate wheel/sdist plus an isolated installed-wheel import before OIDC upload |
| A nested MCP resource URL returns 404 or rejects its browser origin | The HTTP endpoint stayed at /mcp and origin parsing retained a path | Derive the endpoint path and HTTPS origin separately from the configured resource; reject credentials, queries and fragments and exercise authenticated nested routes |
| An automated MCP major update fails import and type checks | SDK v2 removes APIs used by the v1 adapter | Keep the explicit v1 dependency range and ignore automatic major widening while retaining v1 updates; require a tested adapter migration for v2 |
| A green cross-repository demo misses recently merged fixes | Dependency source pins predate the reviewed main commits and documentation lists different pins | Refresh exact final-main source identities together with the documented revision table and verify the combined demo in a fresh environment |
| A lock experiment passes without proving contention | A child readiness marker was written before its SQLite call | Require an observed zero-timeout SQLITE_BUSY/LOCKED probe before releasing the parent lock |
| Benchmark version labels disagree with executed code | Editable source overrides stale installed distribution metadata | Report runtime and installed versions separately and hash the executed source without exposing local paths |

| Concurrent replay duplicates publication and failed acknowledgments never recover | Check/publish lacked a shared claim and success was counted before the acknowledgment; restart lost volatile markers | Serialize per-engine claims or opt into a dedicated SQLite replay store, persist publication before retryable acknowledgment, retain legacy marker reads and test independent runners plus crash redelivery |
| Release workflow uses an advisory-affected or mutable publishing action | A dependency update in one repository left equivalent release workflows unchanged | Pin every publishing workflow to the verified patched action commit and validate each workflow before release |
