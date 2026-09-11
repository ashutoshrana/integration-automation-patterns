"""Optional MCP SDK v1 resource server with verified identity and durable replay protection.

Issuer/public key, resource URL, subject grants and approved permissions are
operator configuration, never values supplied in a tool invocation. Rotate keys
by rebuilding the verifier. TLS termination is a deployment responsibility.
"""

import inspect
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

import jwt
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools.base import Tool
from mcp.server.fastmcp.utilities.func_metadata import FuncMetadata
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from .sqlite_outbox import SQLiteOutbox


class MCPAuditDeliveryError(RuntimeError):
    """Audit acknowledgment failed; event_committed identifies the enqueue boundary."""

    def __init__(self, *, event_committed: bool) -> None:
        self.event_committed = event_committed
        super().__init__(f"Audit acknowledgment failed (event_committed={event_committed})")


class StrictMetadata(FuncMetadata):
    """Do not let SDK convenience parsing coerce JSON strings into objects."""

    def pre_parse_json(self, data: dict[str, Any]) -> dict[str, Any]:
        return data


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    request_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    event_type: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$")
    value: str = Field(max_length=1000)


class JWTVerifier:
    def __init__(self, public_key: str, issuer: str, audience: str, grants: Mapping[str, frozenset[str]]) -> None:
        if any(urlparse(url).scheme != "https" for url in (issuer, audience)):
            raise ValueError("issuer and resource must use HTTPS")
        self.public_key, self.issuer, self.audience = public_key, issuer, audience
        self.grants = dict(grants)

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            claims = jwt.decode(
                token,
                self.public_key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp", "iat", "sub", "aud", "iss"]},
            )
            subject = claims["sub"]
            scope = claims.get("scope", "")
            if not isinstance(subject, str) or not isinstance(scope, str) or subject not in self.grants:
                return None
            scopes = sorted(set(scope.split()) & self.grants[subject])
            return AccessToken(
                token=token,
                client_id=str(claims.get("client_id", subject)),
                subject=subject,
                scopes=scopes,
                expires_at=int(claims["exp"]),
                resource=self.audience,
            )
        except (jwt.PyJWTError, ValueError, TypeError):
            return None


def create_mcp_server(
    store: SQLiteOutbox,
    verifier: JWTVerifier,
    audit_sink: Callable[[dict[str, str]], object],
    *,
    approved_permissions: frozenset[str],
    declared_permissions: frozenset[str] = frozenset({"events:write"}),
) -> FastMCP[Any]:
    """Return an authenticated Streamable HTTP server. Audit contains no arguments/tokens/subject."""
    if declared_permissions != approved_permissions or declared_permissions != frozenset({"events:write"}):
        raise ValueError("manifest permissions differ from operator approval")

    if inspect.iscoroutinefunction(audit_sink) or inspect.iscoroutinefunction(getattr(audit_sink, "__call__", None)):
        raise TypeError("Audit sinks must acknowledge synchronously")

    def emit(decision: str, *, event_committed: bool = False) -> None:
        try:
            acknowledged = audit_sink({"tool": "enqueue_event", "decision": decision})
            if inspect.isawaitable(acknowledged):
                if inspect.iscoroutine(acknowledged):
                    acknowledged.close()
                raise TypeError("Audit sinks must acknowledge synchronously")
        except Exception as exc:
            raise MCPAuditDeliveryError(event_committed=event_committed) from exc

    def enqueue_event(request: EnqueueRequest) -> dict[str, bool]:
        token = get_access_token()
        if token is None or not token.subject or "events:write" not in token.scopes:
            emit("denied")
            raise PermissionError("authenticated events:write permission required")
        emit("authorized")
        # Subject scopes idempotency keys so one user cannot suppress another's request.
        created = store.enqueue(f"{token.subject}:{request.request_id}", request.model_dump())
        emit("created" if created else "replay", event_committed=True)
        return {"created": created}

    tool = Tool.from_function(enqueue_event)
    tool.fn_metadata = StrictMetadata(**tool.fn_metadata.model_dump())
    tool.fn_metadata.arg_model.model_config["extra"] = "forbid"
    tool.fn_metadata.arg_model.model_config["strict"] = True
    tool.fn_metadata.arg_model.model_rebuild(force=True)
    tool.parameters = tool.fn_metadata.arg_model.model_json_schema()
    server: FastMCP[Any] = FastMCP(
        "durable-events",
        tools=[tool],
        token_verifier=verifier,
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[urlparse(verifier.audience).netloc],
            allowed_origins=[verifier.audience.rsplit("/", 1)[0]],
        ),
        auth=AuthSettings(
            issuer_url=AnyHttpUrl(verifier.issuer),
            resource_server_url=AnyHttpUrl(verifier.audience),
            required_scopes=["events:write"],
            validate_token_resource=True,
        ),
    )

    return server
