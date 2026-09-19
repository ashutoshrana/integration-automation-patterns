import json
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from integration_automation_patterns.mcp_server import JWTVerifier, create_mcp_server
from integration_automation_patterns.sqlite_outbox import SQLiteOutbox


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_path", ["/mcp", "/api/events/mcp"])
async def test_sdk_http_auth_schema_and_replay(tmp_path, resource_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    issuer, audience = "https://issuer.example", "https://events.example" + resource_path
    verifier = JWTVerifier(public, issuer, audience, {"alice": frozenset({"events:write"}), "bob": frozenset()})
    store, audits = SQLiteOutbox(tmp_path / "mcp.db"), []
    server = create_mcp_server(store, verifier, audits.append, approved_permissions=frozenset({"events:write"}))
    app = server.streamable_http_app()

    def token(**overrides):
        claims = {
            "sub": "alice",
            "iss": issuer,
            "aud": audience,
            "iat": int(time.time()),
            "exp": int(time.time()) + 60,
            "scope": "events:write",
        }
        claims.update(overrides)
        return jwt.encode(claims, key, algorithm="RS256")

    async with server.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://events.example"
        ) as client:

            async def invoke(arguments, bearer=None, origin="https://events.example"):
                headers = {
                    "Accept": "application/json, text/event-stream",
                    "MCP-Protocol-Version": "2025-11-25",
                    "Origin": origin,
                }
                if bearer:
                    headers["Authorization"] = "Bearer " + bearer
                return await client.post(
                    resource_path,
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "enqueue_event", "arguments": arguments},
                    },
                )

            args = {"request": {"request_id": "id1", "event_type": "created", "value": "sensitive-value"}}
            assert (await invoke(args)).status_code == 401
            assert (await invoke(args, token(aud="https://wrong.example"))).status_code == 401
            assert (await invoke(args, token(sub="bob"))).status_code == 403
            assert (await invoke(args, token(iss="https://untrusted.example"))).status_code == 401
            assert (await invoke(args, token(scope=""))).status_code == 403
            assert (await invoke(args, token(sub="unknown"))).status_code == 401
            assert (await invoke(args, token(exp=int(time.time()) - 10))).status_code == 401
            valid = token()
            assert (await invoke(args, valid, origin="https://untrusted.example")).status_code == 403
            result = await invoke(args, valid)
            assert result.status_code == 200, result.text
            assert not result.json()["result"].get("isError"), result.text
            again = await invoke(args, valid)
            assert again.json()["result"]["structuredContent"] == {"created": False}
            for invalid in [
                {"request": json.dumps(args["request"])},
                {**args, "extra": "blocked"},
                {"request": {**args["request"], "value": {"nested": "bad"}}},
                {"request": {**args["request"], "extra": True}},
                {"request": {**args["request"], "request_id": 7}},
                {"request": {**args["request"], "value": "changed"}},
            ]:
                rejected = await invoke(invalid, valid)
                assert rejected.json()["result"]["isError"], rejected.text
    assert len(store.pending()) == 1
    assert "sensitive-value" not in str(audits)
    assert "alice" not in str(audits)
    with pytest.raises(ValueError, match="permissions"):
        create_mcp_server(
            store,
            verifier,
            audits.append,
            approved_permissions=frozenset({"events:write"}),
            declared_permissions=frozenset({"events:write", "admin"}),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["async_function", "async_callable", "awaitable_result", "before", "after"])
async def test_audit_acknowledgment_enforces_commit_boundary(tmp_path, monkeypatch, failure):
    from mcp.server.auth.provider import AccessToken
    from mcp.server.fastmcp.exceptions import ToolError

    import integration_automation_patterns.mcp_server as module

    verifier = JWTVerifier(
        "unused for verified-token callback test",
        "https://issuer.example",
        "https://events.example/mcp",
        {"alice": frozenset({"events:write"})},
    )
    store = SQLiteOutbox(tmp_path / "audit.db")
    token = AccessToken(token="synthetic", client_id="client", subject="alice", scopes=["events:write"])
    monkeypatch.setattr(module, "get_access_token", lambda: token)

    async def async_sink(record):
        pytest.fail("Unacknowledged coroutine must never run")

    class AsyncSink:
        async def __call__(self, record):
            pytest.fail("Async callable must be rejected")

    def sink(record):
        if failure == "awaitable_result":
            return async_sink(record)
        if failure == "before" or record["decision"] == "created":
            raise OSError("sensitive sink details must not reach the client")

    configured_sink = (
        async_sink if failure == "async_function" else AsyncSink() if failure == "async_callable" else sink
    )
    if failure in {"async_function", "async_callable"}:
        with pytest.raises(TypeError, match="synchronously"):
            create_mcp_server(store, verifier, configured_sink, approved_permissions=frozenset({"events:write"}))
        assert store.pending() == []
        return
    server = create_mcp_server(store, verifier, configured_sink, approved_permissions=frozenset({"events:write"}))
    args = {"request": {"request_id": "id1", "event_type": "test", "value": "synthetic"}}
    with pytest.raises(ToolError, match=f"event_committed={failure == 'after'}") as error:
        await server.call_tool("enqueue_event", args)
    assert "sensitive sink details" not in str(error.value)
    assert len(store.pending()) == int(failure == "after")
    if failure == "after":
        # The same request recovers after the lost post-commit acknowledgment.
        await server.call_tool("enqueue_event", args)
        assert len(store.pending()) == 1


@pytest.mark.parametrize(
    "resource",
    [
        "https:///mcp",
        "https://user:password@events.example/mcp",
        "https://events.example/mcp?tenant=x",
        "https://events.example/mcp#fragment",
    ],
)
def test_invalid_resource_configuration(resource):
    with pytest.raises(ValueError, match="resource"):
        JWTVerifier("unused", "https://issuer.example", resource, {})
