"""
Tests for 25_schema_registry_patterns.py — InMemorySchemaRegistry,
CompatibilityLevel enforcement, AvroSchemaEvolution, ConfluentFramingCodec,
JsonSchemaSerializer, SchemaEvolutionMigrator, and full integration workflow.

38 tests total.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_name = "schema_registry_patterns_25"
_path = os.path.join(os.path.dirname(__file__), "..", "examples", "25_schema_registry_patterns.py")


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
# Helpers for building schema strings
# ---------------------------------------------------------------------------


def _make_schema(*fields) -> str:
    """Build an Avro record schema JSON string from field dicts."""
    return json.dumps(
        {
            "type": "record",
            "name": "TestRecord",
            "fields": list(fields),
        }
    )


_SCHEMA_ID_EMAIL = _make_schema(
    {"name": "id", "type": "int"},
    {"name": "email", "type": "string"},
)

_SCHEMA_WITH_DEFAULT = _make_schema(
    {"name": "id", "type": "int"},
    {"name": "email", "type": "string"},
    {"name": "full_name", "type": "string", "default": ""},
)

_SCHEMA_REQUIRED_NEW_FIELD = _make_schema(
    {"name": "id", "type": "int"},
    {"name": "email", "type": "string"},
    {"name": "required_new", "type": "string"},  # no default
)


# ===========================================================================
# TestInMemorySchemaRegistry  (tests 1–8)
# ===========================================================================


class TestInMemorySchemaRegistry:
    # 1 — register returns an integer schema_id
    def test_register_returns_int(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        sid = registry.register("test-subject", _SCHEMA_ID_EMAIL)
        assert isinstance(sid, int)
        assert sid >= 1

    # 2 — get_schema returns the registered SchemaVersion
    def test_get_schema_by_id(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        sid = registry.register("subj", _SCHEMA_ID_EMAIL)
        sv = registry.get_schema(sid)
        assert sv.schema_id == sid
        assert sv.subject == "subj"
        assert sv.schema_str == _SCHEMA_ID_EMAIL

    # 3 — get_latest_schema returns the most recently registered version
    def test_get_latest_schema(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        registry.register("s", _SCHEMA_ID_EMAIL)
        registry.register("s", _SCHEMA_WITH_DEFAULT)
        latest = registry.get_latest_schema("s")
        assert latest.version == 2
        assert latest.schema_str == _SCHEMA_WITH_DEFAULT

    # 4 — idempotent re-registration returns existing schema_id
    def test_idempotent_reregister(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        sid1 = registry.register("s", _SCHEMA_ID_EMAIL)
        sid2 = registry.register("s", _SCHEMA_ID_EMAIL)
        assert sid1 == sid2
        # Only one version should exist
        assert registry.get_versions("s") == [1]

    # 5 — get_latest_schema raises SchemaRegistryError for unknown subject
    def test_get_latest_unknown_subject(self, m):
        registry = m.InMemorySchemaRegistry()
        with pytest.raises(m.SchemaRegistryError):
            registry.get_latest_schema("does-not-exist")

    # 6 — get_schema raises SchemaRegistryError for unknown id
    def test_get_schema_unknown_id(self, m):
        registry = m.InMemorySchemaRegistry()
        with pytest.raises(m.SchemaRegistryError):
            registry.get_schema(999999)

    # 7 — get_versions returns ordered version numbers
    def test_get_versions(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        registry.register("v", _SCHEMA_ID_EMAIL)
        registry.register("v", _SCHEMA_WITH_DEFAULT)
        assert registry.get_versions("v") == [1, 2]

    # 8 — set_compatibility and get_compatibility round-trip
    def test_set_get_compatibility(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        # default
        assert registry.get_compatibility("any") == m.CompatibilityLevel.BACKWARD
        # override
        registry.set_compatibility("special", m.CompatibilityLevel.FORWARD)
        assert registry.get_compatibility("special") == m.CompatibilityLevel.FORWARD
        # other subjects still use default
        assert registry.get_compatibility("other") == m.CompatibilityLevel.BACKWARD


# ===========================================================================
# TestCompatibilityChecks  (tests 9–13)
# ===========================================================================


class TestCompatibilityChecks:
    # 9 — first registration for a new subject always succeeds
    def test_first_registration_always_succeeds(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        sid = registry.register("brand-new", _SCHEMA_ID_EMAIL)
        assert sid >= 1

    # 10 — backward compat: adding field with default passes
    def test_backward_compat_added_field_with_default(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        registry.register("bc", _SCHEMA_ID_EMAIL)
        # Should succeed — new field has a default
        sid2 = registry.register("bc", _SCHEMA_WITH_DEFAULT)
        assert sid2 >= 1

    # 11 — backward compat: adding required field (no default) fails
    def test_backward_compat_required_new_field_fails(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        registry.register("bc2", _SCHEMA_ID_EMAIL)
        with pytest.raises(m.IncompatibleSchemaError):
            registry.register("bc2", _SCHEMA_REQUIRED_NEW_FIELD)

    # 12 — forward compat: removing required field from writer fails
    def test_forward_compat_removed_required_field_fails(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.FORWARD)
        # Start with the schema that has id + email + full_name(default)
        registry.register("fc", _SCHEMA_WITH_DEFAULT)
        # Remove "email" — old reader needs it, no default in old schema
        schema_without_email = _make_schema(
            {"name": "id", "type": "int"},
            {"name": "full_name", "type": "string", "default": ""},
        )
        with pytest.raises(m.IncompatibleSchemaError):
            registry.register("fc", schema_without_email)

    # 13 — NONE compatibility always passes even for incompatible schemas
    def test_none_compatibility_always_passes(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        registry.register("nc", _SCHEMA_ID_EMAIL)
        # Any schema should register regardless of compatibility
        totally_different = json.dumps(
            {
                "type": "record",
                "name": "Other",
                "fields": [{"name": "x", "type": "string"}],
            }
        )
        sid2 = registry.register("nc", totally_different)
        assert sid2 >= 1


# ===========================================================================
# TestAvroSchemaEvolution  (tests 14–20)
# ===========================================================================


class TestAvroSchemaEvolution:
    # 14 — parse_fields returns a dict keyed by field name
    def test_parse_fields_keys(self, m):
        fields = m.AvroSchemaEvolution.parse_fields(_SCHEMA_ID_EMAIL)
        assert set(fields.keys()) == {"id", "email"}

    # 15 — parse_fields normalises bare string type to dict
    def test_parse_fields_normalises_type(self, m):
        schema = _make_schema({"name": "x", "type": "string"})
        fields = m.AvroSchemaEvolution.parse_fields(schema)
        assert fields["x"]["type"] == {"type": "string"}

    # 16 — backward compatible: adding field with default
    def test_backward_compat_add_field_with_default(self, m):
        assert m.AvroSchemaEvolution.is_backward_compatible(_SCHEMA_WITH_DEFAULT, _SCHEMA_ID_EMAIL) is True

    # 17 — backward compatible: removing a field is OK
    def test_backward_compat_remove_field(self, m):
        schema_no_email = _make_schema({"name": "id", "type": "int"})
        assert m.AvroSchemaEvolution.is_backward_compatible(schema_no_email, _SCHEMA_ID_EMAIL) is True

    # 18 — NOT backward compatible: adding required field (no default)
    def test_backward_not_compat_required_new_field(self, m):
        assert m.AvroSchemaEvolution.is_backward_compatible(_SCHEMA_REQUIRED_NEW_FIELD, _SCHEMA_ID_EMAIL) is False

    # 19 — forward compatible: new writer adds field, old reader ignores it
    def test_forward_compat_new_field_no_default(self, m):
        # writer (_SCHEMA_REQUIRED_NEW_FIELD) has extra required field;
        # old reader (_SCHEMA_ID_EMAIL) ignores it → forward compatible
        assert m.AvroSchemaEvolution.is_forward_compatible(_SCHEMA_REQUIRED_NEW_FIELD, _SCHEMA_ID_EMAIL) is True

    # 20 — full compatible: adding field with default is both backward and forward
    def test_full_compat_add_field_with_default(self, m):
        assert m.AvroSchemaEvolution.is_full_compatible(_SCHEMA_WITH_DEFAULT, _SCHEMA_ID_EMAIL) is True


# ===========================================================================
# TestConfluentFramingCodec  (tests 21–26)
# ===========================================================================


class TestConfluentFramingCodec:
    # 21 — encode produces bytes starting with magic byte 0x00
    def test_encode_starts_with_magic(self, m):
        encoded = m.ConfluentFramingCodec.encode(7, b"hello")
        assert encoded[0] == 0x00

    # 22 — encode/decode round-trip preserves schema_id and payload
    def test_encode_decode_roundtrip(self, m):
        schema_id = 42
        payload = b'{"id": 1}'
        encoded = m.ConfluentFramingCodec.encode(schema_id, payload)
        decoded_id, decoded_payload = m.ConfluentFramingCodec.decode(encoded)
        assert decoded_id == schema_id
        assert decoded_payload == payload

    # 23 — decode extracts correct schema_id for large id
    def test_decode_large_schema_id(self, m):
        schema_id = 100000
        payload = b"data"
        encoded = m.ConfluentFramingCodec.encode(schema_id, payload)
        decoded_id, _ = m.ConfluentFramingCodec.decode(encoded)
        assert decoded_id == schema_id

    # 24 — validate_framing True for valid framing
    def test_validate_framing_true(self, m):
        encoded = m.ConfluentFramingCodec.encode(1, b"payload")
        assert m.ConfluentFramingCodec.validate_framing(encoded) is True

    # 25 — validate_framing False for data shorter than 5 bytes
    def test_validate_framing_false_too_short(self, m):
        assert m.ConfluentFramingCodec.validate_framing(b"\x00\x00") is False

    # 26 — validate_framing False when first byte is not 0x00
    def test_validate_framing_false_bad_magic(self, m):
        bad = b"\x01\x00\x00\x00\x01hello"
        assert m.ConfluentFramingCodec.validate_framing(bad) is False


# ===========================================================================
# TestJsonSchemaSerializer  (tests 27–30)
# ===========================================================================


class TestJsonSchemaSerializer:
    def _setup(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        registry.register("ser-test", _SCHEMA_ID_EMAIL)
        serializer = m.JsonSchemaSerializer(registry)
        return registry, serializer

    # 27 — serialize returns bytes starting with Confluent magic byte
    def test_serialize_starts_with_magic(self, m):
        _, serializer = self._setup(m)
        data = serializer.serialize("ser-test", {"id": 1, "email": "a@b.com"})
        assert data[0] == 0x00

    # 28 — serialize/deserialize round-trip preserves record
    def test_serialize_deserialize_roundtrip(self, m):
        _, serializer = self._setup(m)
        record = {"id": 99, "email": "test@example.com"}
        data = serializer.serialize("ser-test", record)
        restored, _ = serializer.deserialize(data)
        assert restored == record

    # 29 — schema_id embedded in serialized bytes matches registry schema_id
    def test_serialize_embeds_correct_schema_id(self, m):
        registry, serializer = self._setup(m)
        latest = registry.get_latest_schema("ser-test")
        data = serializer.serialize("ser-test", {"id": 1, "email": "x@y.com"})
        _, embedded_id = serializer.deserialize(data)
        assert embedded_id == latest.schema_id

    # 30 — serialize raises SchemaRegistryError for unknown subject
    def test_serialize_unknown_subject_raises(self, m):
        _, serializer = self._setup(m)
        with pytest.raises(m.SchemaRegistryError):
            serializer.serialize("unknown-subject", {"x": 1})


# ===========================================================================
# TestSchemaEvolutionMigrator  (tests 31–35)
# ===========================================================================


class TestSchemaEvolutionMigrator:
    def _setup(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        id_v1 = registry.register("mig", _SCHEMA_ID_EMAIL)
        id_v2 = registry.register("mig", _SCHEMA_WITH_DEFAULT)
        migrator = m.SchemaEvolutionMigrator(registry)
        return registry, migrator, id_v1, id_v2

    # 31 — migrate adds missing fields using their declared defaults
    def test_migrate_adds_missing_field_with_default(self, m):
        _, migrator, id_v1, id_v2 = self._setup(m)
        record = {"id": 1, "email": "a@b.com"}
        migrated = migrator.migrate(record, id_v1, id_v2)
        assert "full_name" in migrated
        assert migrated["full_name"] == ""  # the declared default

    # 32 — migrate removes fields not in target schema
    def test_migrate_removes_extra_fields(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        schema_with_extra = _make_schema(
            {"name": "id", "type": "int"},
            {"name": "email", "type": "string"},
            {"name": "legacy_field", "type": "string", "default": ""},
        )
        id_big = registry.register("rm-test", schema_with_extra)
        id_small = registry.register("rm-test", _SCHEMA_ID_EMAIL)
        migrator = m.SchemaEvolutionMigrator(registry)
        record = {"id": 5, "email": "x@y.com", "legacy_field": "old"}
        migrated = migrator.migrate(record, id_big, id_small)
        assert "legacy_field" not in migrated
        assert migrated["id"] == 5
        assert migrated["email"] == "x@y.com"

    # 33 — migrate to same schema returns equivalent record
    def test_migrate_same_schema_no_change(self, m):
        _, migrator, id_v1, _ = self._setup(m)
        record = {"id": 7, "email": "same@same.com"}
        migrated = migrator.migrate(record, id_v1, id_v1)
        assert migrated == record

    # 34 — migrate does not mutate the original record
    def test_migrate_does_not_mutate_original(self, m):
        _, migrator, id_v1, id_v2 = self._setup(m)
        original = {"id": 3, "email": "orig@test.com"}
        original_copy = dict(original)
        migrator.migrate(original, id_v1, id_v2)
        assert original == original_copy

    # 35 — migrate raises SchemaRegistryError for unknown from_schema_id
    def test_migrate_unknown_from_schema_id(self, m):
        _, migrator, _, _ = self._setup(m)
        with pytest.raises(m.SchemaRegistryError):
            migrator.migrate({"id": 1}, 999999, 1)


# ===========================================================================
# TestIntegration  (tests 36–38)
# ===========================================================================


class TestIntegration:
    # 36 — full workflow: register v1 → v2, check compat, serialize, deserialize
    def test_full_schema_lifecycle(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        serializer = m.JsonSchemaSerializer(registry)

        # Register v1
        id_v1 = registry.register("events-value", _SCHEMA_ID_EMAIL)
        assert id_v1 == 1

        # Register v2 (backward compatible)
        id_v2 = registry.register("events-value", _SCHEMA_WITH_DEFAULT)
        assert id_v2 == 2
        assert registry.get_versions("events-value") == [1, 2]

        # Compatibility check
        assert registry.check_compatibility("events-value", _SCHEMA_WITH_DEFAULT) is True

        # Serialize with v2 as latest
        record = {"id": 10, "email": "user@example.com", "full_name": "Alice"}
        data = serializer.serialize("events-value", record)
        assert m.ConfluentFramingCodec.validate_framing(data)

        # Deserialize and verify
        restored, embedded_id = serializer.deserialize(data)
        assert restored == record
        assert embedded_id == id_v2

    # 37 — v1 record migrated to v2 gets default for new field
    def test_v1_record_migrated_to_v2(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.NONE)
        id_v1 = registry.register("mig2", _SCHEMA_ID_EMAIL)
        id_v2 = registry.register("mig2", _SCHEMA_WITH_DEFAULT)
        migrator = m.SchemaEvolutionMigrator(registry)

        v1_record = {"id": 42, "email": "alice@test.com"}
        migrated = migrator.migrate(v1_record, id_v1, id_v2)

        assert migrated["id"] == 42
        assert migrated["email"] == "alice@test.com"
        assert migrated["full_name"] == ""  # default value from v2 schema

    # 38 — incompatible registration raises IncompatibleSchemaError with subject info
    def test_incompatible_registration_includes_subject(self, m):
        registry = m.InMemorySchemaRegistry(m.CompatibilityLevel.BACKWARD)
        registry.register("guarded", _SCHEMA_ID_EMAIL)

        with pytest.raises(m.IncompatibleSchemaError) as exc_info:
            registry.register("guarded", _SCHEMA_REQUIRED_NEW_FIELD)

        err = exc_info.value
        assert err.subject == "guarded"
        assert "BACKWARD" in err.compatibility
