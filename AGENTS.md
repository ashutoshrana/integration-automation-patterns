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
