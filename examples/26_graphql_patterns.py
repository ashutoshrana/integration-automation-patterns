"""
26_graphql_patterns.py — GraphQL patterns for schema definition, resolver
management, data loading, cursor-based pagination, field-level authorization,
and subscription pub/sub — all implemented with pure Python stdlib.

Demonstrates six complementary patterns that together implement a
production-grade GraphQL infrastructure layer compatible with the
GraphQL June 2018 specification and the Relay Cursor Connection Specification:

    Pattern 1 — GraphQLSchema (schema definition + SDL builder):
                Maintains a registry of named GraphQLObjectType instances and
                generates Schema Definition Language (SDL) strings following
                the October 2021 GraphQL spec §3.6.  Non-nullable fields get
                the ``!`` suffix; list fields are wrapped in ``[...]``.
                Optional triple-quoted docstring block comments are emitted for
                types and fields that carry a description.  This mirrors the
                SDL emitted by graphene's ``print_schema()``, strawberry's
                ``strawberry.Schema.as_str()``, and graphql-core-3's
                ``print_schema()``.

    Pattern 2 — ResolverRegistry (field resolver dispatch):
                Stores per-(type, field) resolver functions and dispatches
                ``resolve(type_name, field_name, parent, args, context)``
                calls exactly as a GraphQL execution engine would.  Uses a
                decorator-based registration API identical to Ariadne's
                ``@query.field("fieldName")`` and graphql-core-3's resolver
                map approach.  Missing resolvers raise ``ResolverError``,
                matching graphql-core-3's ``GraphQLError`` on unresolved
                fields.

    Pattern 3 — DataLoader (N+1 prevention via batched loading):
                Implements the DataLoader algorithm popularised by Facebook's
                dataloader npm package (now also available as
                ``strawberry-graphql-django``'s Django data loader and
                graphql-core-3's ``SyncDataLoader``).  ``load(key)`` enqueues
                a key; ``dispatch()`` calls ``batch_fn`` exactly once for all
                pending keys; ``get(key)`` retrieves the cached result.
                Cache priming (``prime``) and invalidation (``clear``) match
                the DataLoader v2 API.

    Pattern 4 — CursorPagination (Relay Cursor Connection Specification):
                Implements the full Relay Cursor Connection Spec
                (https://relay.dev/graphql/connections.htm) with ``first``,
                ``after``, ``last``, and ``before`` arguments.  Cursors are
                opaque base64-encoded strings (``cursor:<offset>``), matching
                the encoding used by Relay, graphene-django, and
                strawberry-graphql's cursor_based_pagination.  The returned
                ``Connection`` dict includes ``edges``, ``page_info``, and
                ``total_count`` — the standard Relay connection shape.

    Pattern 5 — FieldAuthorizationPolicy (field-level RBAC):
                Registers required-role sets per (type, field) pair and
                evaluates ``check(type_name, field_name, context)`` for
                ResolverContext instances.  Follows the same semantics as
                GraphQL Shield (JS) and strawberry-graphql's ``Permission``
                classes: any one matching role grants access; an absent policy
                grants access by default.

    Pattern 6 — SubscriptionManager (GraphQL pub/sub):
                Implements a thread-safe pub/sub broker for GraphQL
                subscriptions, matching the semantics of graphql-ws and
                graphql-subscriptions (JS).  ``subscribe`` / ``unsubscribe``
                manage per-event-type subscriber callbacks; ``publish``
                delivers payloads synchronously to all registered callbacks
                and returns the delivery count.

No external dependencies required.
"""

from __future__ import annotations

import base64
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class GraphQLScalar(Enum):
    """Built-in GraphQL scalar types.

    Maps each scalar name to its SDL keyword.  These correspond to the five
    built-in scalars defined in the GraphQL June 2018 spec §3.5.
    """

    STRING = "String"
    INT = "Int"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    ID = "ID"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ResolverError(Exception):
    """Raised when the ResolverRegistry cannot dispatch a field resolution.

    Attributes:
        type_name:  The GraphQL type whose field could not be resolved.
        field_name: The field name that was unresolvable.
    """

    def __init__(self, message: str, type_name: str = "", field_name: str = "") -> None:
        super().__init__(message)
        self.type_name = type_name
        self.field_name = field_name


class DataLoaderError(Exception):
    """Raised when accessing a DataLoader key that has not been dispatched yet."""


# ---------------------------------------------------------------------------
# Pattern 1 — GraphQL type definitions
# ---------------------------------------------------------------------------


@dataclass
class GraphQLField:
    """A single field declaration inside a GraphQLObjectType.

    Attributes:
        name:        Field name as it appears in SDL (camelCase by convention).
        type_str:    Base type string, e.g. ``"String"``, ``"Int"``, ``"User"``.
                     Do not include ``!`` or ``[...]`` here -- use *nullable*
                     and *is_list* instead.
        nullable:    If False, the SDL representation appends ``!``.
        description: Optional field description emitted as an inline
                     triple-quoted doc comment in the SDL.
        is_list:     If True, the type is wrapped in ``[...]`` in SDL.
    """

    name: str
    type_str: str
    nullable: bool = True
    description: str = ""
    is_list: bool = False

    def sdl_type(self) -> str:
        """Return the SDL type string, e.g. ``"String"``, ``"[User]!"``.

        Applies list wrapping and non-null modifier in the order required by
        the GraphQL spec (list outer, non-null inner for list elements is
        handled by the caller if needed; here we treat the whole field).
        """
        t = f"[{self.type_str}]" if self.is_list else self.type_str
        if not self.nullable:
            t += "!"
        return t


@dataclass
class GraphQLObjectType:
    """A GraphQL object type (``type Foo { ... }``).

    Attributes:
        name:        Type name as it appears in SDL (PascalCase by convention).
        fields:      Ordered list of GraphQLField declarations.
        description: Optional type description emitted as a docstring block
                     comment before the ``type`` keyword in the SDL.
    """

    name: str
    fields: list[GraphQLField]
    description: str = ""


# ---------------------------------------------------------------------------
# Pattern 1 — GraphQLSchema
# ---------------------------------------------------------------------------


class GraphQLSchema:
    """In-memory GraphQL schema registry with SDL generation.

    Stores GraphQLObjectType instances by name and generates a Schema
    Definition Language string on demand.  The SDL output matches the format
    produced by graphene, strawberry, and graphql-core-3's ``print_schema()``.

    Usage::

        schema = GraphQLSchema()
        schema.register_type(GraphQLObjectType("User", [
            GraphQLField("id", "ID", nullable=False),
            GraphQLField("name", "String"),
        ]))
        print(schema.build_sdl())
    """

    def __init__(self) -> None:
        self._types: dict[str, GraphQLObjectType] = {}

    def register_type(self, obj_type: GraphQLObjectType) -> None:
        """Register a GraphQLObjectType in the schema.

        Overwrites any previously registered type with the same name.

        Args:
            obj_type: The object type to register.
        """
        self._types[obj_type.name] = obj_type

    def get_type(self, name: str) -> GraphQLObjectType:
        """Return the registered GraphQLObjectType for *name*.

        Args:
            name: The type name to look up.

        Returns:
            The matching GraphQLObjectType.

        Raises:
            KeyError: If no type with that name is registered.
        """
        if name not in self._types:
            raise KeyError(f"Type not found in schema: {name!r}")
        return self._types[name]

    def list_types(self) -> list[str]:
        """Return a list of all registered type names.

        Returns:
            List of type name strings in insertion order.
        """
        return list(self._types.keys())

    def build_sdl(self) -> str:
        """Build and return the Schema Definition Language string.

        Each registered type is emitted as::

            \"\"\"Description.\"\"\"
            type TypeName {
              \"\"\"Field description.\"\"\"
              fieldName: FieldType
            }

        Returns:
            SDL string with a trailing newline between type blocks.
        """
        blocks: list[str] = []
        for obj_type in self._types.values():
            lines: list[str] = []
            if obj_type.description:
                lines.append(f'"""{obj_type.description}"""')
            lines.append(f"type {obj_type.name} {{")
            for f in obj_type.fields:
                if f.description:
                    lines.append(f'  """{f.description}"""')
                lines.append(f"  {f.name}: {f.sdl_type()}")
            lines.append("}")
            blocks.append("\n".join(lines))
        return "\n\n".join(blocks)


# ---------------------------------------------------------------------------
# Pattern 2 — ResolverContext
# ---------------------------------------------------------------------------


@dataclass
class ResolverContext:
    """Execution context passed to every resolver function.

    Attributes:
        user_id:    Identifier of the authenticated user.
        roles:      Immutable set of role names the user holds.
        request_id: Unique ID for the current GraphQL request (useful for
                    tracing and logging).
        metadata:   Arbitrary extra key-value pairs (e.g. tracing spans,
                    feature flags).
    """

    user_id: str
    roles: frozenset[str]
    request_id: str
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pattern 2 — ResolverRegistry
# ---------------------------------------------------------------------------


class ResolverRegistry:
    """Decorator-based registry for GraphQL field resolvers.

    Each resolver is a callable with the signature::

        def resolver(parent: dict, args: dict, context: ResolverContext) -> Any

    The registry dispatches ``resolve()`` calls to the matching resolver,
    raising ``ResolverError`` when no resolver is registered for a
    ``(type_name, field_name)`` pair — mirroring graphql-core-3 behaviour.

    Usage::

        registry = ResolverRegistry()

        @registry.register("Query", "users")
        def resolve_users(parent, args, context):
            return fetch_all_users()
    """

    def __init__(self) -> None:
        self._resolvers: dict[tuple[str, str], Callable] = {}

    def register(self, type_name: str, field_name: str) -> Callable:
        """Return a decorator that registers the decorated function as a resolver.

        Args:
            type_name:  The GraphQL type (e.g. ``"Query"``).
            field_name: The field on that type (e.g. ``"users"``).

        Returns:
            A decorator; the decorated function is stored and returned
            unchanged so the function can still be called directly.
        """
        def decorator(fn: Callable) -> Callable:
            self._resolvers[(type_name, field_name)] = fn
            return fn
        return decorator

    def resolve(
        self,
        type_name: str,
        field_name: str,
        parent: dict,
        args: dict,
        context: ResolverContext,
    ) -> Any:
        """Dispatch a resolver call for ``(type_name, field_name)``.

        Args:
            type_name:  The GraphQL type being resolved.
            field_name: The field being resolved.
            parent:     The parent object (result of the enclosing resolver).
            args:       Field arguments from the GraphQL query.
            context:    The current ResolverContext.

        Returns:
            Whatever the registered resolver function returns.

        Raises:
            ResolverError: If no resolver is registered for the pair.
        """
        key = (type_name, field_name)
        if key not in self._resolvers:
            raise ResolverError(
                f"No resolver registered for {type_name}.{field_name}",
                type_name=type_name,
                field_name=field_name,
            )
        return self._resolvers[key](parent, args, context)

    def list_resolvers(self) -> list[tuple[str, str]]:
        """Return all registered ``(type_name, field_name)`` pairs.

        Returns:
            List of ``(type_name, field_name)`` tuples in insertion order.
        """
        return list(self._resolvers.keys())

    def has_resolver(self, type_name: str, field_name: str) -> bool:
        """Return True if a resolver is registered for ``(type_name, field_name)``.

        Args:
            type_name:  The GraphQL type.
            field_name: The field name.

        Returns:
            True if a resolver exists, False otherwise.
        """
        return (type_name, field_name) in self._resolvers


# ---------------------------------------------------------------------------
# Pattern 3 — DataLoader
# ---------------------------------------------------------------------------


class DataLoader:
    """Batch-loading cache for N+1 prevention in GraphQL resolvers.

    Implements the DataLoader algorithm: keys are enqueued via ``load()``,
    then ``dispatch()`` calls ``batch_fn`` exactly once with all pending keys
    and caches results.  Subsequent ``get()`` calls return cached values.

    The API mirrors the DataLoader v2 npm package and is compatible with
    strawberry-graphql-django's ``DataLoader`` interface.

    Args:
        batch_fn: Callable accepting a list of keys and returning a dict
                  mapping each key to its resolved value.

    Usage::

        loader = DataLoader(batch_fn=lambda keys: {k: db.get(k) for k in keys})
        loader.load("user:1")
        loader.load("user:2")
        loader.dispatch()
        print(loader.get("user:1"))
    """

    def __init__(self, batch_fn: Callable[[list], dict]) -> None:
        self._batch_fn = batch_fn
        self._cache: dict = {}
        self._pending: list = []  # preserves order; deduplication in load()

    def load(self, key: Any) -> None:
        """Enqueue *key* for batch loading.

        If the key is already cached or already pending, it is not added again
        (deduplication).

        Args:
            key: The key to load (must be hashable).
        """
        if key not in self._cache and key not in self._pending:
            self._pending.append(key)

    def dispatch(self) -> None:
        """Execute the batch function for all pending keys.

        Calls ``batch_fn`` exactly once with all enqueued keys, stores every
        result in the cache, and clears the pending queue.  Subsequent calls
        to ``dispatch()`` with no new ``load()`` calls are no-ops.
        """
        if not self._pending:
            return
        results = self._batch_fn(list(self._pending))
        self._cache.update(results)
        self._pending.clear()

    def get(self, key: Any) -> Any:
        """Return the cached value for *key*.

        Args:
            key: A previously loaded and dispatched key.

        Returns:
            The resolved value from the batch function.

        Raises:
            KeyError: If the key has not been loaded and dispatched yet.
        """
        if key not in self._cache:
            raise KeyError(f"Key not loaded: {key!r}")
        return self._cache[key]

    def clear(self, key: Any = None) -> None:
        """Invalidate cache entries.

        Args:
            key: If provided, remove only this key from the cache.
                 If ``None``, clear the entire cache.
        """
        if key is None:
            self._cache.clear()
        else:
            self._cache.pop(key, None)

    def prime(self, key: Any, value: Any) -> None:
        """Pre-populate the cache with a known value.

        Useful in tests or when the value is already available without
        a round-trip through ``batch_fn``.

        Args:
            key:   The cache key.
            value: The value to associate with *key*.
        """
        self._cache[key] = value


# ---------------------------------------------------------------------------
# Pattern 4 — CursorPagination (Relay Cursor Connection Spec)
# ---------------------------------------------------------------------------


class CursorPagination:
    """Cursor-based pagination following the Relay Cursor Connection Spec.

    Cursors are opaque base64-encoded strings in the form ``cursor:<offset>``.
    The ``paginate()`` method supports all four Relay pagination arguments:
    ``first``, ``after``, ``last``, and ``before``.

    References:
        https://relay.dev/graphql/connections.htm
        https://graphql.org/learn/pagination/

    All methods are static — this class is a namespace and holds no state.
    """

    _PREFIX = "cursor:"

    @staticmethod
    def encode_cursor(offset: int) -> str:
        """Encode an integer offset as an opaque cursor string.

        Args:
            offset: The 0-based list index to encode.

        Returns:
            URL-safe base64 string (no padding characters).
        """
        raw = f"{CursorPagination._PREFIX}{offset}".encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")

    @staticmethod
    def decode_cursor(cursor: str) -> int:
        """Decode an opaque cursor string to an integer offset.

        Args:
            cursor: A cursor string produced by ``encode_cursor()``.

        Returns:
            The integer offset encoded in the cursor.

        Raises:
            ValueError: If the cursor is not a valid ``cursor:<int>`` token.
        """
        try:
            raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        except Exception as exc:
            raise ValueError(f"Invalid cursor encoding: {cursor!r}") from exc
        if not raw.startswith(CursorPagination._PREFIX):
            raise ValueError(
                f"Invalid cursor format (missing prefix): {cursor!r}"
            )
        offset_str = raw[len(CursorPagination._PREFIX):]
        try:
            return int(offset_str)
        except ValueError:
            raise ValueError(
                f"Invalid cursor format (non-integer offset): {cursor!r}"
            )

    @staticmethod
    def paginate(
        items: list,
        first: int | None = None,
        after: str | None = None,
        last: int | None = None,
        before: str | None = None,
    ) -> dict:
        """Paginate a list of items using Relay cursor arguments.

        Each edge in the result contains ``{"node": item, "cursor": str}``.
        ``page_info`` follows the Relay Connection Spec fields.

        Args:
            items:  The full list of items to paginate.
            first:  Maximum number of items to return from the start of the
                    (optionally offset) slice.
            after:  Return items *after* this cursor (exclusive).
            last:   Maximum number of items to return from the *end* of the
                    slice.
            before: Return items *before* this cursor (exclusive).

        Returns:
            A dict with keys ``"edges"``, ``"page_info"``, and
            ``"total_count"``.
        """
        total = len(items)
        encode = CursorPagination.encode_cursor
        decode = CursorPagination.decode_cursor

        # Determine the start and end indices of the working slice.
        start_index = 0
        end_index = total

        if after is not None:
            start_index = decode(after) + 1
        if before is not None:
            end_index = decode(before)

        # Apply first / last slicing within the window.
        if first is not None:
            end_index = min(end_index, start_index + first)
        if last is not None:
            start_index = max(start_index, end_index - last)

        # Clamp to valid range.
        start_index = max(0, start_index)
        end_index = min(total, end_index)

        slice_items = items[start_index:end_index]
        edges = [
            {"node": item, "cursor": encode(start_index + i)}
            for i, item in enumerate(slice_items)
        ]

        has_previous = start_index > 0
        has_next = end_index < total
        start_cursor = edges[0]["cursor"] if edges else None
        end_cursor = edges[-1]["cursor"] if edges else None

        return {
            "edges": edges,
            "page_info": {
                "has_next_page": has_next,
                "has_previous_page": has_previous,
                "start_cursor": start_cursor,
                "end_cursor": end_cursor,
            },
            "total_count": total,
        }


# ---------------------------------------------------------------------------
# Pattern 5 — FieldAuthorizationPolicy
# ---------------------------------------------------------------------------


class FieldAuthorizationPolicy:
    """Field-level role-based access control for GraphQL resolvers.

    Stores required-role sets per ``(type_name, field_name)`` pair and
    evaluates access for a given ``ResolverContext``.  If no policy is
    registered for a field, access is granted (open by default), matching
    the behaviour of GraphQL Shield's ``allow`` rule.

    Usage::

        policy = FieldAuthorizationPolicy()
        policy.require_roles("User", "ssn", {"admin", "compliance"})
        ctx = ResolverContext(user_id="u1", roles=frozenset({"admin"}), ...)
        assert policy.check("User", "ssn", ctx) is True
    """

    def __init__(self) -> None:
        self._policies: dict[tuple[str, str], set[str]] = {}

    def require_roles(self, type_name: str, field_name: str, roles: set[str]) -> None:
        """Register a required-role set for a field.

        Access is granted when the ``ResolverContext`` holds **any** of the
        specified roles (OR semantics, matching GraphQL Shield).

        Args:
            type_name:  The GraphQL type.
            field_name: The field name.
            roles:      Set of role names; at least one must be present.
        """
        self._policies[(type_name, field_name)] = set(roles)

    def check(self, type_name: str, field_name: str, context: ResolverContext) -> bool:
        """Return True if *context* is authorised to access the field.

        Args:
            type_name:  The GraphQL type.
            field_name: The field name.
            context:    The current ResolverContext.

        Returns:
            True if no policy is registered (open) or if the context holds at
            least one required role; False otherwise.
        """
        key = (type_name, field_name)
        if key not in self._policies:
            return True
        required = self._policies[key]
        return bool(required & context.roles)

    def get_required_roles(self, type_name: str, field_name: str) -> set[str]:
        """Return the set of roles required for a field.

        Args:
            type_name:  The GraphQL type.
            field_name: The field name.

        Returns:
            The required role set, or an empty set if no policy is registered.
        """
        return set(self._policies.get((type_name, field_name), set()))


# ---------------------------------------------------------------------------
# Pattern 6 — SubscriptionManager
# ---------------------------------------------------------------------------


class SubscriptionManager:
    """Thread-safe pub/sub broker for GraphQL subscriptions.

    Implements the event-type → subscriber-id → callback dispatch pattern
    used by graphql-subscriptions (JS) and graphql-ws.  All mutations to
    internal state are protected by a ``threading.Lock``.

    Usage::

        manager = SubscriptionManager()
        manager.subscribe("USER_CREATED", "sub-1", lambda p: print(p))
        count = manager.publish("USER_CREATED", {"userId": "u42"})
        assert count == 1
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # event_type → {subscriber_id: callback}
        self._subscribers: dict[str, dict[str, Callable]] = {}

    def subscribe(
        self, event_type: str, subscriber_id: str, callback: Callable
    ) -> None:
        """Register *callback* to receive events of *event_type*.

        If *subscriber_id* is already subscribed to *event_type*, the
        existing callback is replaced.

        Args:
            event_type:    The event category to subscribe to.
            subscriber_id: A unique identifier for this subscription.
            callback:      Callable invoked with the event payload dict.
        """
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = {}
            self._subscribers[event_type][subscriber_id] = callback

    def unsubscribe(self, subscriber_id: str, event_type: str | None = None) -> None:
        """Remove a subscriber from one or all event types.

        Args:
            subscriber_id: The subscriber to remove.
            event_type:    If provided, remove only this event type
                           subscription.  If ``None``, remove the subscriber
                           from every event type they are registered for.
        """
        with self._lock:
            if event_type is not None:
                if event_type in self._subscribers:
                    self._subscribers[event_type].pop(subscriber_id, None)
            else:
                for subs in self._subscribers.values():
                    subs.pop(subscriber_id, None)

    def publish(self, event_type: str, payload: dict) -> int:
        """Deliver *payload* to all subscribers of *event_type*.

        Each subscriber callback is called synchronously with ``payload``.
        Callbacks are called outside the lock to avoid deadlocks in
        single-threaded tests.

        Args:
            event_type: The event category to publish to.
            payload:    The event data dict delivered to callbacks.

        Returns:
            The number of subscribers the payload was delivered to.
        """
        with self._lock:
            callbacks = list(self._subscribers.get(event_type, {}).values())
        for cb in callbacks:
            cb(payload)
        return len(callbacks)

    def subscriber_count(self, event_type: str) -> int:
        """Return the number of active subscribers for *event_type*.

        Args:
            event_type: The event category.

        Returns:
            Integer count of current subscribers.
        """
        with self._lock:
            return len(self._subscribers.get(event_type, {}))


# ---------------------------------------------------------------------------
# __main__ — end-to-end demonstration
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("=" * 70)
    print("GraphQL Patterns — end-to-end workflow demo")
    print("=" * 70)

    # -----------------------------------------------------------------------
    # Step 1 — Build a GraphQL schema with User and Post types
    # -----------------------------------------------------------------------
    schema = GraphQLSchema()

    user_type = GraphQLObjectType(
        name="User",
        description="A registered user of the platform.",
        fields=[
            GraphQLField("id", "ID", nullable=False, description="Unique user ID"),
            GraphQLField("name", "String"),
            GraphQLField("email", "String", nullable=False),
            GraphQLField("posts", "Post", is_list=True),
        ],
    )

    post_type = GraphQLObjectType(
        name="Post",
        description="A blog post authored by a user.",
        fields=[
            GraphQLField("id", "ID", nullable=False),
            GraphQLField("title", "String", nullable=False),
            GraphQLField("body", "String"),
            GraphQLField("authorId", "ID", nullable=False),
        ],
    )

    schema.register_type(user_type)
    schema.register_type(post_type)

    print("\n[Step 1] Schema SDL:")
    print(schema.build_sdl())
    print(f"  Registered types: {schema.list_types()}")

    # -----------------------------------------------------------------------
    # Step 2 — Register resolvers for Query.users and User.posts
    # -----------------------------------------------------------------------
    _users_db = [
        {"id": "u1", "name": "Alice", "email": "alice@example.com"},
        {"id": "u2", "name": "Bob",   "email": "bob@example.com"},
    ]
    _posts_db = {
        "u1": [{"id": "p1", "title": "Hello World", "body": "First post!", "authorId": "u1"}],
        "u2": [{"id": "p2", "title": "GraphQL Tips", "body": "Use DataLoader!", "authorId": "u2"}],
    }

    registry = ResolverRegistry()

    @registry.register("Query", "users")
    def resolve_users(parent, args, context):
        return _users_db

    @registry.register("User", "posts")
    def resolve_user_posts(parent, args, context):
        return _posts_db.get(parent["id"], [])

    ctx = ResolverContext(
        user_id="system",
        roles=frozenset({"admin"}),
        request_id="req-demo-001",
    )

    users = registry.resolve("Query", "users", {}, {}, ctx)
    print(f"\n[Step 2] Resolved Query.users: {[u['name'] for u in users]}")
    posts_for_alice = registry.resolve("User", "posts", users[0], {}, ctx)
    print(f"  Alice's posts: {[p['title'] for p in posts_for_alice]}")
    print(f"  Registered resolvers: {registry.list_resolvers()}")

    # -----------------------------------------------------------------------
    # Step 3 — Use DataLoader to batch-load user posts
    # -----------------------------------------------------------------------
    batch_call_count = [0]

    def batch_load_posts(user_ids: list) -> dict:
        batch_call_count[0] += 1
        return {uid: _posts_db.get(uid, []) for uid in user_ids}

    loader = DataLoader(batch_fn=batch_load_posts)
    for user in _users_db:
        loader.load(user["id"])

    loader.dispatch()
    print(f"\n[Step 3] DataLoader — batch_fn called {batch_call_count[0]} time(s) "
          f"for {len(_users_db)} users")
    for user in _users_db:
        posts = loader.get(user["id"])
        print(f"  {user['name']}: {len(posts)} post(s)")

    # -----------------------------------------------------------------------
    # Step 4 — Paginate a list of 20 items (first=5, then last=5)
    # -----------------------------------------------------------------------
    twenty_items = [{"id": i, "value": f"item-{i}"} for i in range(20)]
    pager = CursorPagination()

    result_first5 = pager.paginate(twenty_items, first=5)
    print(f"\n[Step 4] Pagination — first=5 of 20:")
    print(f"  edges count     : {len(result_first5['edges'])}")
    print(f"  has_next_page   : {result_first5['page_info']['has_next_page']}")
    print(f"  has_prev_page   : {result_first5['page_info']['has_previous_page']}")
    print(f"  total_count     : {result_first5['total_count']}")

    end_cursor = result_first5["page_info"]["end_cursor"]
    result_next5 = pager.paginate(twenty_items, first=5, after=end_cursor)
    print(f"  After end of first page — next 5: items "
          f"{[e['node']['id'] for e in result_next5['edges']]}")

    result_last5 = pager.paginate(twenty_items, last=5)
    print(f"  last=5 of 20: items {[e['node']['id'] for e in result_last5['edges']]}")

    # -----------------------------------------------------------------------
    # Step 5 — Check field authorization
    # -----------------------------------------------------------------------
    auth_policy = FieldAuthorizationPolicy()
    auth_policy.require_roles("User", "email", {"admin", "self"})
    auth_policy.require_roles("User", "ssn",   {"admin"})

    admin_ctx = ResolverContext("u0", frozenset({"admin"}), "req-admin")
    user_ctx  = ResolverContext("u1", frozenset({"self"}),  "req-self")
    anon_ctx  = ResolverContext("",   frozenset(),           "req-anon")

    print("\n[Step 5] Field Authorization:")
    print(f"  admin  → User.email : {auth_policy.check('User', 'email', admin_ctx)}")
    print(f"  self   → User.email : {auth_policy.check('User', 'email', user_ctx)}")
    print(f"  anon   → User.email : {auth_policy.check('User', 'email', anon_ctx)}")
    print(f"  admin  → User.ssn   : {auth_policy.check('User', 'ssn', admin_ctx)}")
    print(f"  self   → User.ssn   : {auth_policy.check('User', 'ssn', user_ctx)}")
    print(f"  anyone → User.name  : {auth_policy.check('User', 'name', anon_ctx)} (no policy)")

    # -----------------------------------------------------------------------
    # Step 6 — Subscription pub/sub
    # -----------------------------------------------------------------------
    sub_manager = SubscriptionManager()
    received: list[dict] = []

    sub_manager.subscribe("USER_CREATED", "dashboard", lambda p: received.append(p))
    sub_manager.subscribe("USER_CREATED", "audit-log", lambda p: None)

    count = sub_manager.publish("USER_CREATED", {"userId": "u3", "name": "Carol"})
    print(f"\n[Step 6] Subscriptions — published to {count} subscriber(s)")
    print(f"  dashboard received: {received}")
    print(f"  subscriber_count('USER_CREATED'): "
          f"{sub_manager.subscriber_count('USER_CREATED')}")

    sub_manager.unsubscribe("audit-log")
    print(f"  After unsubscribe audit-log: "
          f"{sub_manager.subscriber_count('USER_CREATED')} subscriber(s)")

    print("\nDone.")
