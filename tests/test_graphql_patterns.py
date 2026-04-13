"""
Tests for 26_graphql_patterns.py — GraphQLSchema, ResolverRegistry,
DataLoader, CursorPagination, FieldAuthorizationPolicy, SubscriptionManager.

38 tests total.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_name = "graphql_patterns_26"
_path = os.path.join(os.path.dirname(__file__), "..", "examples", "26_graphql_patterns.py")


def _load():
    if _name in sys.modules:
        return sys.modules[_name]
    spec = importlib.util.spec_from_file_location(_name, _path)
    mod = types.ModuleType(_name)
    sys.modules[_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(m, roles=("viewer",), user_id="u1", request_id="req-test"):
    return m.ResolverContext(
        user_id=user_id,
        roles=frozenset(roles),
        request_id=request_id,
    )


def _user_type(m):
    return m.GraphQLObjectType(
        name="User",
        fields=[
            m.GraphQLField("id", "ID", nullable=False),
            m.GraphQLField("name", "String"),
            m.GraphQLField("email", "String", nullable=False),
        ],
    )


def _post_type(m):
    return m.GraphQLObjectType(
        name="Post",
        fields=[
            m.GraphQLField("id", "ID", nullable=False),
            m.GraphQLField("title", "String", nullable=False),
            m.GraphQLField("tags", "String", is_list=True),
        ],
    )


# ===========================================================================
# TestGraphQLSchema  (tests 1–8)
# ===========================================================================


class TestGraphQLSchema:
    # 1 — register_type and get_type round-trip
    def test_register_and_get_type(self, m):
        schema = m.GraphQLSchema()
        user = _user_type(m)
        schema.register_type(user)
        retrieved = schema.get_type("User")
        assert retrieved.name == "User"
        assert len(retrieved.fields) == 3

    # 2 — get_type raises KeyError for unknown type
    def test_get_type_unknown_raises_key_error(self, m):
        schema = m.GraphQLSchema()
        with pytest.raises(KeyError):
            schema.get_type("DoesNotExist")

    # 3 — list_types returns registered names
    def test_list_types(self, m):
        schema = m.GraphQLSchema()
        schema.register_type(_user_type(m))
        schema.register_type(_post_type(m))
        names = schema.list_types()
        assert "User" in names
        assert "Post" in names
        assert len(names) == 2

    # 4 — build_sdl includes type name
    def test_build_sdl_includes_type_name(self, m):
        schema = m.GraphQLSchema()
        schema.register_type(_user_type(m))
        sdl = schema.build_sdl()
        assert "type User" in sdl

    # 5 — build_sdl includes field names
    def test_build_sdl_includes_fields(self, m):
        schema = m.GraphQLSchema()
        schema.register_type(_user_type(m))
        sdl = schema.build_sdl()
        assert "id:" in sdl or "id: " in sdl
        assert "name:" in sdl or "name: " in sdl
        assert "email:" in sdl or "email: " in sdl

    # 6 — non-nullable field gets ! in SDL
    def test_build_sdl_non_nullable_field_gets_bang(self, m):
        schema = m.GraphQLSchema()
        schema.register_type(_user_type(m))
        sdl = schema.build_sdl()
        # id and email are nullable=False
        assert "ID!" in sdl
        assert "String!" in sdl

    # 7 — list field gets bracket wrapping in SDL
    def test_build_sdl_list_field_gets_brackets(self, m):
        schema = m.GraphQLSchema()
        schema.register_type(_post_type(m))
        sdl = schema.build_sdl()
        assert "[String]" in sdl

    # 8 — type description appears as docstring block in SDL
    def test_build_sdl_type_description(self, m):
        schema = m.GraphQLSchema()
        typed = m.GraphQLObjectType(
            name="Widget",
            description="A reusable widget.",
            fields=[m.GraphQLField("id", "ID")],
        )
        schema.register_type(typed)
        sdl = schema.build_sdl()
        assert "A reusable widget." in sdl


# ===========================================================================
# TestResolverRegistry  (tests 9–16)
# ===========================================================================


class TestResolverRegistry:
    # 9 — register decorator and resolve call return resolver result
    def test_register_and_resolve(self, m):
        registry = m.ResolverRegistry()
        ctx = _make_context(m)

        @registry.register("Query", "ping")
        def resolve_ping(parent, args, context):
            return "pong"

        result = registry.resolve("Query", "ping", {}, {}, ctx)
        assert result == "pong"

    # 10 — missing resolver raises ResolverError
    def test_missing_resolver_raises_resolver_error(self, m):
        registry = m.ResolverRegistry()
        ctx = _make_context(m)
        with pytest.raises(m.ResolverError):
            registry.resolve("Query", "missing", {}, {}, ctx)

    # 11 — list_resolvers returns registered (type, field) pairs
    def test_list_resolvers(self, m):
        registry = m.ResolverRegistry()

        @registry.register("Query", "users")
        def r1(parent, args, ctx):
            return []

        @registry.register("User", "name")
        def r2(parent, args, ctx):
            return "Alice"

        pairs = registry.list_resolvers()
        assert ("Query", "users") in pairs
        assert ("User", "name") in pairs

    # 12 — has_resolver True for registered field
    def test_has_resolver_true(self, m):
        registry = m.ResolverRegistry()

        @registry.register("Mutation", "createUser")
        def r(parent, args, ctx):
            return {}

        assert registry.has_resolver("Mutation", "createUser") is True

    # 13 — has_resolver False for unregistered field
    def test_has_resolver_false(self, m):
        registry = m.ResolverRegistry()
        assert registry.has_resolver("Query", "notRegistered") is False

    # 14 — resolver receives parent argument
    def test_resolver_receives_parent(self, m):
        registry = m.ResolverRegistry()
        ctx = _make_context(m)
        captured = {}

        @registry.register("User", "fullName")
        def r(parent, args, context):
            captured["parent"] = parent
            return parent.get("first") + " " + parent.get("last")

        registry.resolve("User", "fullName", {"first": "John", "last": "Doe"}, {}, ctx)
        assert captured["parent"] == {"first": "John", "last": "Doe"}

    # 15 — resolver receives args argument
    def test_resolver_receives_args(self, m):
        registry = m.ResolverRegistry()
        ctx = _make_context(m)
        captured = {}

        @registry.register("Query", "user")
        def r(parent, args, context):
            captured["args"] = args
            return {"id": args["id"]}

        registry.resolve("Query", "user", {}, {"id": "u42"}, ctx)
        assert captured["args"] == {"id": "u42"}

    # 16 — resolver receives context argument
    def test_resolver_receives_context(self, m):
        registry = m.ResolverRegistry()
        ctx = _make_context(m, roles=("admin",), user_id="u99")
        captured = {}

        @registry.register("Query", "me")
        def r(parent, args, context):
            captured["context"] = context
            return {"id": context.user_id}

        registry.resolve("Query", "me", {}, {}, ctx)
        assert captured["context"].user_id == "u99"
        assert "admin" in captured["context"].roles


# ===========================================================================
# TestDataLoader  (tests 17–24)
# ===========================================================================


class TestDataLoader:
    # 17 — load and dispatch, then get returns value
    def test_load_dispatch_get(self, m):
        loader = m.DataLoader(batch_fn=lambda keys: {k: k * 2 for k in keys})
        loader.load(3)
        loader.dispatch()
        assert loader.get(3) == 6

    # 18 — batch_fn called exactly once for multiple loads
    def test_batch_fn_called_once(self, m):
        call_count = [0]

        def batch(keys):
            call_count[0] += 1
            return {k: k for k in keys}

        loader = m.DataLoader(batch_fn=batch)
        loader.load("a")
        loader.load("b")
        loader.load("c")
        loader.dispatch()
        assert call_count[0] == 1

    # 19 — duplicate load() calls are deduplicated
    def test_load_deduplicates_keys(self, m):
        received = []

        def batch(keys):
            received.extend(keys)
            return {k: k for k in keys}

        loader = m.DataLoader(batch_fn=batch)
        loader.load("x")
        loader.load("x")  # duplicate
        loader.load("x")  # duplicate
        loader.dispatch()
        assert received.count("x") == 1

    # 20 — prime pre-populates cache (no dispatch needed)
    def test_prime_pre_populates_cache(self, m):
        loader = m.DataLoader(batch_fn=lambda keys: {})
        loader.prime("primed-key", "primed-value")
        assert loader.get("primed-key") == "primed-value"

    # 21 — clear specific key removes it from cache
    def test_clear_specific_key(self, m):
        loader = m.DataLoader(batch_fn=lambda keys: {k: k for k in keys})
        loader.load("k1")
        loader.load("k2")
        loader.dispatch()
        loader.clear("k1")
        with pytest.raises(KeyError):
            loader.get("k1")
        assert loader.get("k2") == "k2"

    # 22 — clear all removes entire cache
    def test_clear_all(self, m):
        loader = m.DataLoader(batch_fn=lambda keys: {k: k for k in keys})
        loader.load("a")
        loader.load("b")
        loader.dispatch()
        loader.clear()
        with pytest.raises(KeyError):
            loader.get("a")

    # 23 — get before dispatch raises KeyError
    def test_get_before_dispatch_raises(self, m):
        loader = m.DataLoader(batch_fn=lambda keys: {k: k for k in keys})
        loader.load("unloaded")
        with pytest.raises(KeyError):
            loader.get("unloaded")

    # 24 — primed key is not added to pending list
    def test_prime_prevents_pending(self, m):
        calls = []

        def batch(keys):
            calls.extend(keys)
            return {k: k for k in keys}

        loader = m.DataLoader(batch_fn=batch)
        loader.prime("already", "known")
        loader.load("already")  # already in cache via prime; should not be pending
        loader.dispatch()
        assert "already" not in calls


# ===========================================================================
# TestCursorPagination  (tests 25–34)
# ===========================================================================


class TestCursorPagination:
    # 25 — encode/decode cursor round-trip
    def test_encode_decode_roundtrip(self, m):
        pager = m.CursorPagination()
        for offset in [0, 1, 10, 99, 500]:
            cursor = pager.encode_cursor(offset)
            assert pager.decode_cursor(cursor) == offset

    # 26 — invalid cursor raises ValueError
    def test_invalid_cursor_raises_value_error(self, m):
        pager = m.CursorPagination()
        with pytest.raises(ValueError):
            pager.decode_cursor("not-a-valid-cursor!!!!")

    # 27 — paginate with first=N returns N items from start
    def test_paginate_first_n(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        result = pager.paginate(items, first=3)
        assert len(result["edges"]) == 3
        assert result["edges"][0]["node"] == 0
        assert result["edges"][2]["node"] == 2

    # 28 — paginate after cursor skips correct items
    def test_paginate_after_cursor(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        # First page: first=3 → items 0,1,2; end_cursor is cursor for index 2
        first_page = pager.paginate(items, first=3)
        end_cursor = first_page["page_info"]["end_cursor"]
        second_page = pager.paginate(items, first=3, after=end_cursor)
        nodes = [e["node"] for e in second_page["edges"]]
        assert nodes == [3, 4, 5]

    # 29 — paginate last=N returns N items from end
    def test_paginate_last_n(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        result = pager.paginate(items, last=3)
        nodes = [e["node"] for e in result["edges"]]
        assert nodes == [7, 8, 9]

    # 30 — paginate before cursor excludes items at or after cursor
    def test_paginate_before_cursor(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        # Encode cursor for offset 5 → before=5 means items 0..4
        cursor_5 = pager.encode_cursor(5)
        result = pager.paginate(items, before=cursor_5)
        nodes = [e["node"] for e in result["edges"]]
        assert nodes == [0, 1, 2, 3, 4]

    # 31 — no pagination args returns all items
    def test_no_args_returns_all(self, m):
        items = list(range(5))
        pager = m.CursorPagination()
        result = pager.paginate(items)
        assert len(result["edges"]) == 5

    # 32 — total_count is always full list length
    def test_total_count(self, m):
        items = list(range(20))
        pager = m.CursorPagination()
        result = pager.paginate(items, first=5)
        assert result["total_count"] == 20

    # 33 — has_next_page is True when more items remain
    def test_page_info_has_next_page(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        result = pager.paginate(items, first=5)
        assert result["page_info"]["has_next_page"] is True

    # 34 — has_previous_page is True on second page
    def test_page_info_has_previous_page(self, m):
        items = list(range(10))
        pager = m.CursorPagination()
        first_end = pager.paginate(items, first=3)["page_info"]["end_cursor"]
        second_page = pager.paginate(items, first=3, after=first_end)
        assert second_page["page_info"]["has_previous_page"] is True


# ===========================================================================
# TestFieldAuthorizationPolicy  (tests 35–38 span into subscription tests)
# ===========================================================================


class TestFieldAuthorizationPolicy:
    # 35 — no policy registered → access granted
    def test_no_policy_allows_access(self, m):
        policy = m.FieldAuthorizationPolicy()
        ctx = _make_context(m, roles=())
        assert policy.check("User", "name", ctx) is True

    # 36 — context has matching role → access granted
    def test_matching_role_allows_access(self, m):
        policy = m.FieldAuthorizationPolicy()
        policy.require_roles("User", "ssn", {"admin"})
        ctx = _make_context(m, roles=("admin",))
        assert policy.check("User", "ssn", ctx) is True

    # 37 — context lacks required role → access denied
    def test_missing_role_denies_access(self, m):
        policy = m.FieldAuthorizationPolicy()
        policy.require_roles("User", "ssn", {"admin"})
        ctx = _make_context(m, roles=("viewer",))
        assert policy.check("User", "ssn", ctx) is False

    # 38 — any of multiple required roles grants access
    def test_any_required_role_grants_access(self, m):
        policy = m.FieldAuthorizationPolicy()
        policy.require_roles("User", "email", {"admin", "self", "support"})
        ctx_admin = _make_context(m, roles=("admin",))
        ctx_support = _make_context(m, roles=("support",))
        ctx_anon = _make_context(m, roles=())
        assert policy.check("User", "email", ctx_admin) is True
        assert policy.check("User", "email", ctx_support) is True
        assert policy.check("User", "email", ctx_anon) is False

    # 39 — get_required_roles returns registered set
    def test_get_required_roles(self, m):
        policy = m.FieldAuthorizationPolicy()
        policy.require_roles("Query", "adminStats", {"admin", "superuser"})
        roles = policy.get_required_roles("Query", "adminStats")
        assert roles == {"admin", "superuser"}

    # 40 — get_required_roles returns empty set for unregistered field
    def test_get_required_roles_unregistered(self, m):
        policy = m.FieldAuthorizationPolicy()
        assert policy.get_required_roles("Query", "publicField") == set()


# ===========================================================================
# TestSubscriptionManager  (tests 41–46)
# ===========================================================================


class TestSubscriptionManager:
    # 41 — subscribe and publish delivers payload to subscriber
    def test_subscribe_and_publish(self, m):
        manager = m.SubscriptionManager()
        received = []
        manager.subscribe("USER_CREATED", "sub-1", lambda p: received.append(p))
        manager.publish("USER_CREATED", {"userId": "u1"})
        assert len(received) == 1
        assert received[0] == {"userId": "u1"}

    # 42 — unsubscribe stops delivery to that subscriber
    def test_unsubscribe_stops_delivery(self, m):
        manager = m.SubscriptionManager()
        received = []
        manager.subscribe("ORDER_PLACED", "sub-a", lambda p: received.append(p))
        manager.unsubscribe("sub-a", "ORDER_PLACED")
        manager.publish("ORDER_PLACED", {"orderId": "o99"})
        assert received == []

    # 43 — publish returns number of subscribers delivered to
    def test_publish_returns_count(self, m):
        manager = m.SubscriptionManager()
        manager.subscribe("TICK", "s1", lambda p: None)
        manager.subscribe("TICK", "s2", lambda p: None)
        manager.subscribe("TICK", "s3", lambda p: None)
        count = manager.publish("TICK", {})
        assert count == 3

    # 44 — subscriber_count returns correct number
    def test_subscriber_count(self, m):
        manager = m.SubscriptionManager()
        assert manager.subscriber_count("NEW_EVENT") == 0
        manager.subscribe("NEW_EVENT", "alpha", lambda p: None)
        manager.subscribe("NEW_EVENT", "beta", lambda p: None)
        assert manager.subscriber_count("NEW_EVENT") == 2

    # 45 — unsubscribe with no event_type removes subscriber from all events
    def test_unsubscribe_all_events(self, m):
        manager = m.SubscriptionManager()
        manager.subscribe("EVT_A", "global-sub", lambda p: None)
        manager.subscribe("EVT_B", "global-sub", lambda p: None)
        manager.unsubscribe("global-sub")
        assert manager.subscriber_count("EVT_A") == 0
        assert manager.subscriber_count("EVT_B") == 0

    # 46 — publish to event with no subscribers returns 0 and does not raise
    def test_publish_no_subscribers_returns_zero(self, m):
        manager = m.SubscriptionManager()
        count = manager.publish("GHOST_EVENT", {"data": "payload"})
        assert count == 0
