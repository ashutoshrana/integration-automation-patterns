"""
http/fastapi_router.py — FastAPI integration for enterprise webhook patterns.

Provides ``WebhookRouter``, a factory that produces a FastAPI ``APIRouter``
pre-wired with HMAC-verified, idempotency-safe webhook endpoints using
``IdempotentWebhookReceiver``.

Two layers of async support:
  1. **Async endpoint** — ``POST /{path}`` is a native ``async def`` route so
     FastAPI can run it on the event loop without a thread-pool hop.
  2. **Async handler** — ``handler`` callbacks can be plain callables OR
     coroutines; ``WebhookRouter`` detects this at registration time with
     ``inspect.iscoroutinefunction`` and awaits accordingly.

This keeps the hot path (signature verification + duplicate check) on the
event loop while still supporting sync legacy handlers.

Usage::

    from fastapi import FastAPI
    from integration_automation_patterns.http.fastapi_router import WebhookRouter

    router = WebhookRouter(secret="whsec_abc123")

    @router.on("github", idempotency_header="X-GitHub-Delivery",
               signature_header="X-Hub-Signature-256")
    async def handle_github(payload: dict) -> None:
        print("GitHub event:", payload)

    @router.on("salesforce", idempotency_header="X-Salesforce-Delivery-Id")
    def handle_salesforce(payload: dict) -> None:
        ...

    app = FastAPI()
    app.include_router(router.build(), prefix="/webhooks")
    # Routes:  POST /webhooks/github
    #          POST /webhooks/salesforce

Installation::

    pip install 'integration-automation-patterns[fastapi]'
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


def _check_fastapi_available() -> None:
    """Raise a clear ImportError when fastapi is not installed."""
    try:
        import fastapi  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "fastapi is required for WebhookRouter. "
            "Install it with: pip install 'integration-automation-patterns[fastapi]'"
        ) from exc


@dataclass
class _RouteConfig:
    """Internal registration for a single webhook endpoint."""

    path: str
    handler: Callable[..., Any]
    idempotency_header: str
    signature_header: str
    seen_cache_size: int


class WebhookRouter:
    """
    Factory that produces a FastAPI ``APIRouter`` with HMAC-verified,
    idempotency-safe webhook routes.

    Each route registered via ``@router.on(...)`` gets an ``async def``
    FastAPI endpoint that:

    1. Reads the raw request body (preserving bytes for HMAC verification).
    2. Delegates to ``IdempotentWebhookReceiver.receive``.
    3. Returns **400** on invalid signature.
    4. Returns **200 "duplicate"** for replayed events (idempotent skip).
    5. Awaits the registered handler (coroutine or sync).
    6. Returns **200 "ok"** on success.
    7. Returns **500** (with ``X-Webhook-Error`` header) on handler exception.

    Args:
        secret: HMAC shared secret used to verify all routes on this router.
            Override per-route by passing ``secret`` to ``router.on()``.
        default_signature_header: Default header name for the HMAC signature.
            GitHub uses ``X-Hub-Signature-256``; Stripe uses
            ``Stripe-Signature``.  Default: ``"X-Hub-Signature-256"``.
        default_seen_cache_size: LRU cache size for the per-route idempotency
            seen-key store.  Default: ``1024``.
    """

    def __init__(
        self,
        secret: str,
        default_signature_header: str = "X-Hub-Signature-256",
        default_seen_cache_size: int = 1024,
    ) -> None:
        _check_fastapi_available()
        self._secret = secret
        self._default_signature_header = default_signature_header
        self._default_seen_cache_size = default_seen_cache_size
        self._routes: list[_RouteConfig] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def on(
        self,
        path: str,
        *,
        idempotency_header: str,
        signature_header: str | None = None,
        seen_cache_size: int | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator that registers ``handler`` as the processor for ``POST /{path}``.

        Args:
            path: URL path segment (no leading slash). Becomes the route
                ``POST /{path}`` under whatever ``prefix`` the router is
                mounted with.
            idempotency_header: HTTP header containing the unique event ID
                (e.g. ``"X-GitHub-Delivery"``).
            signature_header: HMAC signature header name.  Defaults to
                ``self.default_signature_header``.
            seen_cache_size: Overrides ``default_seen_cache_size`` for this
                route only.

        Returns:
            The original handler, unmodified (decorator passthrough).
        """

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            self._routes.append(
                _RouteConfig(
                    path=path,
                    handler=handler,
                    idempotency_header=idempotency_header,
                    signature_header=signature_header or self._default_signature_header,
                    seen_cache_size=seen_cache_size or self._default_seen_cache_size,
                )
            )
            return handler

        return decorator

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self) -> Any:  # returns fastapi.APIRouter
        """
        Construct and return a ``fastapi.APIRouter`` with all registered routes.

        Call ``app.include_router(router.build(), prefix="/webhooks")`` after
        all ``@router.on(...)`` decorators have run.

        Returns:
            A ``fastapi.APIRouter`` instance ready to be mounted on a FastAPI app.
        """
        from fastapi import APIRouter, Request
        from fastapi.responses import JSONResponse

        from integration_automation_patterns.http import (
            IdempotentWebhookReceiver,
        )

        api_router = APIRouter()

        for cfg in self._routes:
            receiver = IdempotentWebhookReceiver(
                secret=self._secret,
                idempotency_header=cfg.idempotency_header,
                signature_header_name=cfg.signature_header,
                seen_cache_size=cfg.seen_cache_size,
            )
            handler = cfg.handler
            route_path = f"/{cfg.path}"
            is_async = inspect.iscoroutinefunction(handler)

            # Capture loop variables in closure
            def _make_endpoint(
                _receiver: IdempotentWebhookReceiver,
                _handler: Callable[..., Any],
                _is_async: bool,
                _path: str,
            ) -> Callable[..., Any]:
                async def endpoint(request: Request) -> JSONResponse:
                    body = await request.body()
                    headers = dict(request.headers)

                    result = _receiver.receive(payload=body, headers=headers)

                    if not result.is_valid:
                        logger.warning(
                            "Webhook signature invalid: path=%s error=%s",
                            _path,
                            result.error,
                        )
                        return JSONResponse(
                            status_code=400,
                            content={"detail": result.error or "invalid signature"},
                        )

                    if result.is_duplicate:
                        logger.debug(
                            "Webhook duplicate skipped: path=%s key=%s",
                            _path,
                            result.idempotency_key,
                        )
                        return JSONResponse(
                            status_code=200,
                            content={"status": "duplicate", "idempotency_key": result.idempotency_key},
                        )

                    try:
                        if _is_async:
                            await _handler(result.payload)
                        else:
                            _handler(result.payload)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Webhook handler raised: path=%s exc=%s", _path, exc)
                        return JSONResponse(
                            status_code=500,
                            content={"detail": "handler error"},
                            headers={"X-Webhook-Error": str(exc)},
                        )

                    return JSONResponse(
                        status_code=200,
                        content={"status": "ok", "idempotency_key": result.idempotency_key},
                    )

                return endpoint

            endpoint_fn = _make_endpoint(receiver, handler, is_async, cfg.path)
            endpoint_fn.__name__ = f"webhook_{cfg.path.replace('/', '_')}"
            api_router.add_api_route(route_path, endpoint_fn, methods=["POST"])
            logger.debug("Registered webhook route: POST %s", route_path)

        return api_router
