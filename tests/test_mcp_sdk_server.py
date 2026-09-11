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
async def test_sdk_http_auth_schema_and_replay(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    issuer, audience = "https://issuer.example", "https://events.example/mcp"
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

            async def invoke(arguments, bearer=None):
                headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-11-25"}
                if bearer:
                    headers["Authorization"] = "Bearer " + bearer
                return await client.post(
                    "/mcp",
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
