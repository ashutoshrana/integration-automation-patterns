"""
Tests for 21_api_gateway_patterns.py — rate limiting, request/response
transformation, API version routing, and API composition (scatter-gather).
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "21_api_gateway_patterns.py"
)


def _load_module():
    module_name = "api_gateway_patterns_21"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(m, client_id="client-1", method="GET", path="/api/users",
                  version="v1", headers=None, body=None):
    return m.APIRequest(
        client_id=client_id,
        method=method,
        path=path,
        version=version,
        headers=headers or {},
        body=body or {},
        timestamp=time.time(),
    )


def _make_response(m, status_code=200, headers=None, body=None,
                   latency_ms=10.0, backend_calls=1):
    return m.APIResponse(
        status_code=status_code,
        headers=headers or {},
        body=body or {},
        latency_ms=latency_ms,
        backend_calls=backend_calls,
    )



# ===========================================================================
# TestRateLimiter
# ===========================================================================


class TestRateLimiter:
    """Tests for RateLimiter: token bucket per-client rate limiting."""

    def test_first_request_within_burst_succeeds(self, m):
        """First consume() on a fresh client should not raise."""
        cfg = m.RateLimitConfig(requests_per_second=5.0, burst_size=3)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-a")

    def test_exceeding_burst_raises_rate_limit_exceeded(self, m):
        """Consuming more tokens than burst_size should raise RateLimitExceeded."""
        cfg = m.RateLimitConfig(requests_per_second=1.0, burst_size=2)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-b")
        limiter.consume("client-b")
        with pytest.raises(m.RateLimitExceeded):
            limiter.consume("client-b")

    def test_rate_limit_exceeded_has_client_id_attribute(self, m):
        """RateLimitExceeded exception should carry the client_id."""
        cfg = m.RateLimitConfig(requests_per_second=1.0, burst_size=1)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-c")
        with pytest.raises(m.RateLimitExceeded) as exc_info:
            limiter.consume("client-c")
        assert exc_info.value.client_id == "client-c"

    def test_rate_limit_exceeded_has_retry_after_seconds_attribute(self, m):
        """RateLimitExceeded exception should carry a positive retry_after_seconds."""
        cfg = m.RateLimitConfig(requests_per_second=2.0, burst_size=1)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-d")
        with pytest.raises(m.RateLimitExceeded) as exc_info:
            limiter.consume("client-d")
        assert exc_info.value.retry_after_seconds > 0.0

    def test_different_clients_have_independent_buckets(self, m):
        """Exhausting one client's tokens should not affect a different client."""
        cfg = m.RateLimitConfig(requests_per_second=1.0, burst_size=1)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-x")
        with pytest.raises(m.RateLimitExceeded):
            limiter.consume("client-x")
        # client-y still has a full bucket — should not raise
        limiter.consume("client-y")

    def test_get_token_count_returns_positive_after_creation(self, m):
        """A freshly-seen client should have a positive token count."""
        cfg = m.RateLimitConfig(requests_per_second=5.0, burst_size=10)
        limiter = m.RateLimiter(cfg)
        count = limiter.get_token_count("client-new")
        assert count > 0.0

    def test_token_count_decreases_after_consume(self, m):
        """Token count should drop by ~1 after each consume()."""
        cfg = m.RateLimitConfig(requests_per_second=5.0, burst_size=10)
        limiter = m.RateLimiter(cfg)
        before = limiter.get_token_count("client-e")
        limiter.consume("client-e")
        after = limiter.get_token_count("client-e")
        assert after < before

    def test_error_message_mentions_client_id(self, m):
        """str(RateLimitExceeded) should contain the client_id."""
        cfg = m.RateLimitConfig(requests_per_second=1.0, burst_size=1)
        limiter = m.RateLimiter(cfg)
        limiter.consume("client-f")
        with pytest.raises(m.RateLimitExceeded) as exc_info:
            limiter.consume("client-f")
        assert "client-f" in str(exc_info.value)


# ===========================================================================
# TestRequestTransformer
# ===========================================================================


class TestRequestTransformer:
    """Tests for RequestTransformer: header injection and body field renaming."""

    def test_add_header_rule_injects_header(self, m):
        """add_header_rule should inject the named header into transformed request."""
        rt = m.RequestTransformer()
        rt.add_header_rule("X-Correlation-ID", "corr-123")
        req = _make_request(m)
        new_req = rt.transform(req)
        assert new_req.headers.get("X-Correlation-ID") == "corr-123"

    def test_multiple_header_rules_all_applied(self, m):
        """All registered header rules should appear in the transformed request."""
        rt = m.RequestTransformer()
        rt.add_header_rule("X-Header-A", "value-a")
        rt.add_header_rule("X-Header-B", "value-b")
        req = _make_request(m)
        new_req = rt.transform(req)
        assert new_req.headers.get("X-Header-A") == "value-a"
        assert new_req.headers.get("X-Header-B") == "value-b"

    def test_add_body_mapping_renames_field(self, m):
        """add_body_mapping should rename the source field to the target field."""
        rt = m.RequestTransformer()
        rt.add_body_mapping("userId", "user_id")
        req = _make_request(m, body={"userId": "abc123", "name": "Alice"})
        new_req = rt.transform(req)
        assert "user_id" in new_req.body
        assert "userId" not in new_req.body

    def test_original_request_not_mutated(self, m):
        """transform() must not modify the original request's headers or body."""
        rt = m.RequestTransformer()
        rt.add_header_rule("X-Injected", "yes")
        rt.add_body_mapping("oldKey", "newKey")
        req = _make_request(m, headers={"Content-Type": "application/json"},
                            body={"oldKey": "value"})
        rt.transform(req)
        assert "X-Injected" not in req.headers
        assert "oldKey" in req.body
        assert "newKey" not in req.body

    def test_original_headers_preserved_after_transform(self, m):
        """Existing headers on the request should still be present after transform."""
        rt = m.RequestTransformer()
        rt.add_header_rule("X-New-Header", "new-value")
        req = _make_request(m, headers={"Authorization": "Bearer token123"})
        new_req = rt.transform(req)
        assert new_req.headers.get("Authorization") == "Bearer token123"
        assert new_req.headers.get("X-New-Header") == "new-value"


# ===========================================================================
# TestResponseTransformer
# ===========================================================================


class TestResponseTransformer:
    """Tests for ResponseTransformer: status remapping and body masking."""

    def test_add_status_mapping_remaps_status_code(self, m):
        """add_status_mapping should translate the from_code to to_code."""
        rt = m.ResponseTransformer()
        rt.add_status_mapping(404, 200)
        resp = _make_response(m, status_code=404, body={"error": "not found"})
        new_resp = rt.transform(resp)
        assert new_resp.status_code == 200

    def test_add_body_mask_replaces_field_with_stars(self, m):
        """add_body_mask should replace the field value with '***'."""
        rt = m.ResponseTransformer()
        rt.add_body_mask("password")
        resp = _make_response(m, body={"user": "alice", "password": "secret"})
        new_resp = rt.transform(resp)
        assert new_resp.body["password"] == "***"

    def test_original_response_not_mutated(self, m):
        """transform() must not modify the original response's body or status."""
        rt = m.ResponseTransformer()
        rt.add_status_mapping(500, 200)
        rt.add_body_mask("token")
        resp = _make_response(m, status_code=500,
                               body={"token": "abc", "data": "value"})
        rt.transform(resp)
        assert resp.status_code == 500
        assert resp.body["token"] == "abc"

    def test_multiple_masks_applied_to_same_response(self, m):
        """All registered mask rules should be applied in a single transform."""
        rt = m.ResponseTransformer()
        rt.add_body_mask("ssn")
        rt.add_body_mask("credit_card")
        resp = _make_response(m, body={"ssn": "123-45-6789",
                                        "credit_card": "4111-1111",
                                        "name": "Alice"})
        new_resp = rt.transform(resp)
        assert new_resp.body["ssn"] == "***"
        assert new_resp.body["credit_card"] == "***"
        assert new_resp.body["name"] == "Alice"

    def test_unmapped_status_code_passes_through_unchanged(self, m):
        """Status codes without a registered mapping should be returned as-is."""
        rt = m.ResponseTransformer()
        rt.add_status_mapping(404, 200)
        resp = _make_response(m, status_code=201)
        new_resp = rt.transform(resp)
        assert new_resp.status_code == 201


# ===========================================================================
# TestAPIVersionRouter
# ===========================================================================


class TestAPIVersionRouter:
    """Tests for APIVersionRouter: version-based request dispatching."""

    def test_route_dispatches_to_correct_handler(self, m):
        """route() should call the handler registered for request.version."""
        router = m.APIVersionRouter()
        v1_response = _make_response(m, body={"version": "v1"})
        router.register("v1", lambda req: v1_response)
        req = _make_request(m, version="v1")
        result = router.route(req)
        assert result is v1_response

    def test_unregistered_version_raises_unsupported_version(self, m):
        """route() with an unregistered version should raise UnsupportedVersion."""
        router = m.APIVersionRouter()
        router.register("v1", lambda req: _make_response(m))
        req = _make_request(m, version="v99")
        with pytest.raises(m.UnsupportedVersion):
            router.route(req)

    def test_unsupported_version_has_version_attribute(self, m):
        """UnsupportedVersion exception should carry the requested version."""
        router = m.APIVersionRouter()
        req = _make_request(m, version="v5")
        with pytest.raises(m.UnsupportedVersion) as exc_info:
            router.route(req)
        assert exc_info.value.version == "v5"

    def test_unsupported_version_has_supported_attribute(self, m):
        """UnsupportedVersion exception should carry the list of supported versions."""
        router = m.APIVersionRouter()
        router.register("v1", lambda req: _make_response(m))
        router.register("v2", lambda req: _make_response(m))
        req = _make_request(m, version="v9")
        with pytest.raises(m.UnsupportedVersion) as exc_info:
            router.route(req)
        assert "v1" in exc_info.value.supported
        assert "v2" in exc_info.value.supported

    def test_supported_versions_returns_registered_versions(self, m):
        """supported_versions property should list all registered version strings."""
        router = m.APIVersionRouter()
        router.register("v1", lambda req: _make_response(m))
        router.register("v2", lambda req: _make_response(m))
        router.register("v3", lambda req: _make_response(m))
        versions = router.supported_versions
        assert set(versions) == {"v1", "v2", "v3"}

    def test_multiple_versions_registered_independently(self, m):
        """Each registered version should dispatch to its own handler."""
        router = m.APIVersionRouter()
        router.register("v1", lambda req: _make_response(m, body={"v": 1}))
        router.register("v2", lambda req: _make_response(m, body={"v": 2}))
        req_v1 = _make_request(m, version="v1")
        req_v2 = _make_request(m, version="v2")
        assert router.route(req_v1).body["v"] == 1
        assert router.route(req_v2).body["v"] == 2

    def test_handler_return_value_passed_through_unchanged(self, m):
        """The exact response returned by the handler should reach the caller."""
        expected = _make_response(m, status_code=201, body={"created": True},
                                   latency_ms=42.0)
        router = m.APIVersionRouter()
        router.register("v1", lambda req: expected)
        req = _make_request(m, version="v1")
        result = router.route(req)
        assert result is expected


# ===========================================================================
# TestAPIComposer
# ===========================================================================


class TestAPIComposer:
    """Tests for APIComposer: sequential and parallel backend fan-out."""

    def test_compose_merges_responses_from_two_backends(self, m):
        """compose() with two backends should return a single merged response."""
        composer = m.APIComposer()
        composer.add_backend("users", lambda req: _make_response(
            m, body={"name": "Alice"}, latency_ms=10.0))
        composer.add_backend("orders", lambda req: _make_response(
            m, body={"count": 5}, latency_ms=20.0))
        req = _make_request(m)
        result = composer.compose(req)
        assert result is not None
        assert isinstance(result.body, dict)

    def test_compose_body_contains_keys_from_both_backends(self, m):
        """Merged body should contain prefixed keys from each backend."""
        composer = m.APIComposer()
        composer.add_backend("alpha", lambda req: _make_response(
            m, body={"x": 1}))
        composer.add_backend("beta", lambda req: _make_response(
            m, body={"y": 2}))
        req = _make_request(m)
        result = composer.compose(req)
        assert "alpha.x" in result.body
        assert "beta.y" in result.body

    def test_compose_backend_calls_equals_number_of_backends(self, m):
        """backend_calls in merged response should reflect the number of backends."""
        composer = m.APIComposer()
        composer.add_backend("svc1", lambda req: _make_response(
            m, backend_calls=1))
        composer.add_backend("svc2", lambda req: _make_response(
            m, backend_calls=1))
        req = _make_request(m)
        result = composer.compose(req)
        assert result.backend_calls == 2

    def test_compose_parallel_returns_merged_result(self, m):
        """compose_parallel() should return a merged response with body keys from all backends."""
        composer = m.APIComposer()
        composer.add_backend("p1", lambda req: _make_response(
            m, body={"a": 10}))
        composer.add_backend("p2", lambda req: _make_response(
            m, body={"b": 20}))
        req = _make_request(m)
        result = composer.compose_parallel(req)
        assert "p1.a" in result.body
        assert "p2.b" in result.body
        assert result.backend_calls > 0

    def test_empty_composer_returns_empty_response(self, m):
        """A composer with no backends should return a response with an empty body."""
        composer = m.APIComposer()
        req = _make_request(m)
        result = composer.compose(req)
        assert result.body == {}
        assert result.backend_calls == 0

    def test_compose_parallel_same_shape_as_sequential(self, m):
        """compose_parallel() body keys should match those from compose() for the same backends."""
        def make_composer():
            c = m.APIComposer()
            c.add_backend("svc_a", lambda req: _make_response(
                m, body={"val": 99}))
            c.add_backend("svc_b", lambda req: _make_response(
                m, body={"flag": True}))
            return c

        req = _make_request(m)
        seq_result = make_composer().compose(req)
        par_result = make_composer().compose_parallel(req)
        assert set(seq_result.body.keys()) == set(par_result.body.keys())


