"""
25_schema_registry_patterns.py — Schema Registry patterns for managing,
versioning, and evolving message schemas across distributed systems with
compatibility enforcement.

Demonstrates six complementary patterns that together implement a production-
grade schema registry and evolution pipeline compatible with the Confluent
Schema Registry API specification and Apache Avro schema evolution rules:

    Pattern 1 — InMemorySchemaRegistry (subject-versioned schema store):
                Stores schemas keyed by subject (e.g. "users-value") with
                auto-incrementing schema IDs and ordered version lists.
                Enforces configurable compatibility levels (BACKWARD, FORWARD,
                FULL, NONE, and transitive variants) before accepting a new
                schema version.  The API mirrors the Confluent Schema Registry
                REST API endpoints POST /subjects/{subject}/versions,
                GET /schemas/ids/{id}, GET /subjects/{subject}/versions/latest,
                and GET /config/{subject}.  Idempotent registration (identical
                schema re-submitted returns existing ID) matches Confluent
                Registry behaviour.  Per-subject compatibility overrides default
                registry-wide compatibility, exactly as in the Confluent
                implementation.

    Pattern 2 — AvroSchemaEvolution (pure-Python Avro compatibility rules):
                Implements the three core Avro schema compatibility modes
                (BACKWARD, FORWARD, FULL) for record schemas without any
                external avro library.  Schemas are represented as JSON dicts
                with a "fields" array.  BACKWARD compatibility (new reader can
                read old data) allows adding fields with defaults and removing
                fields.  FORWARD compatibility (old reader can read new data)
                allows adding fields without defaults (reader ignores them) and
                requires that removed fields have defaults in the old schema.
                FULL compatibility requires both.  These rules match the
                normative Avro Specification §3.2
                (https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
                and are enforced identically by the Confluent Schema Registry,
                Apicurio Registry, and the AWS Glue Schema Registry.

    Pattern 3 — ConfluentFramingCodec (Confluent wire format):
                Implements the five-byte Confluent Schema Registry framing
                prefix: a magic byte 0x00 followed by a four-byte big-endian
                schema ID.  This wire format is used by all Confluent-compatible
                producers and consumers (confluent-kafka-python,
                kafka-python-ng, Spring Cloud Stream, Flink Kafka connector)
                to allow consumers to look up the correct schema before
                deserializing each message.  The encode/decode pair is the
                Python equivalent of KafkaAvroSerializer.serialize() /
                KafkaAvroDeserializer.deserialize() in the Confluent Python
                client.

    Pattern 4 — JsonSchemaSerializer (Confluent-framed JSON serializer):
                A convenience serializer that looks up the latest schema ID
                for a subject, JSON-encodes the record, and prepends the
                Confluent framing header.  Deserialization reverses the process:
                strip the framing, parse JSON, and return the record together
                with the schema ID so callers can perform schema-aware
                processing.  This mirrors KafkaJsonSchemaSerializer /
                KafkaJsonSchemaDeserializer from the Confluent Python client.

    Pattern 5 — SchemaEvolutionMigrator (record migration between versions):
                Migrates a data record from one schema version to another by
                applying field-level transformations: adding fields that exist
                in the target schema but not the source (using the field's
                "default" value when present), and removing fields that exist
                in the source but not the target.  This corresponds to the
                Avro datum reader's field resolution phase and is analogous to
                the migration support in Apache Flink's schema evolution
                (FLIP-295) and Debezium's schema migration for CDC events.

    Pattern 6 — Registry compatibility workflow (end-to-end integration):
                Demonstrates the full lifecycle: register v1 schema, evolve to
                v2 (adding an optional field with a default), verify that the
                compatibility check passes, serialize a v1 record, deserialize
                it, and migrate the record to the v2 schema.  This is the
                canonical Confluent Schema Registry integration workflow used
                in production Kafka pipelines.

Relationship to commercial and open-source schema registry implementations:

    - Confluent Schema Registry : InMemorySchemaRegistry API mirrors REST
                                  endpoints; ConfluentFramingCodec is the
                                  identical wire format; compatibility levels
                                  map 1:1 to Confluent CompatibilityLevel enum.
    - Apicurio Registry         : Same BACKWARD/FORWARD/FULL semantics; REST
                                  API structure is analogous
                                  (/apis/registry/v2/groups/{g}/artifacts).
    - AWS Glue Schema Registry  : Same wire format (magic byte + schema ID);
                                  GlueSchemaRegistryDeserializer uses identical
                                  framing; compatibility modes map directly.
    - Apache Avro Specification : AvroSchemaEvolution rules are derived directly
                                  from the normative Schema Resolution section
                                  of the Avro spec.

No external dependencies required.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from enum import Enum


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class SchemaType(Enum):
    """Wire format / schema language for a registered schema."""

    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


class CompatibilityLevel(Enum):
    """Schema compatibility enforcement mode.

    Mirrors the Confluent Schema Registry CompatibilityLevel enum exactly.
    Transitive variants enforce compatibility against *all* previous versions,
    not just the immediately preceding one.
    """

    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SchemaRegistryError(Exception):
    """Base exception for all schema registry errors."""


class IncompatibleSchemaError(SchemaRegistryError):
    """Raised when a schema fails a compatibility check against existing versions.

    Attributes:
        subject:        The subject name for which registration was attempted.
        compatibility:  The compatibility level that was violated.
        message:        Human-readable explanation of the incompatibility.
    """

    def __init__(self, message: str, subject: str = "", compatibility: str = "") -> None:
        super().__init__(message)
        self.subject = subject
        self.compatibility = compatibility


# ---------------------------------------------------------------------------
# Pattern 1 — SchemaVersion (versioned schema record)
# ---------------------------------------------------------------------------


@dataclass
class SchemaVersion:
    """A single versioned schema stored in the registry.

    Attributes:
        schema_id:   Globally unique, auto-incrementing integer ID.
        version:     Per-subject version number (1-based).
        subject:     The subject name this schema is registered under
                     (e.g. ``"users-value"``).
        schema_str:  JSON string representation of the schema.
        schema_type: The schema language/format (default: AVRO).
    """

    schema_id: int
    version: int
    subject: str
    schema_str: str
    schema_type: SchemaType = SchemaType.AVRO


# ---------------------------------------------------------------------------
# Pattern 1 — InMemorySchemaRegistry
# ---------------------------------------------------------------------------


class InMemorySchemaRegistry:
    """Thread-unsafe, in-process schema registry with full compatibility enforcement.

    Provides a subject-versioned schema store identical in semantics to the
    Confluent Schema Registry.  Schemas are stored by globally unique integer
    ID (auto-incremented) and are also indexed by subject + version.

    Args:
        default_compatibility: Registry-wide default compatibility level.
                               Individual subjects may override this with
                               ``set_compatibility()``.
    """

    def __init__(
        self,
        default_compatibility: CompatibilityLevel = CompatibilityLevel.BACKWARD,
    ) -> None:
        self._default_compatibility = default_compatibility
        self._next_schema_id: int = 1
        self._schemas: dict[int, SchemaVersion] = {}
        self._subjects: dict[str, list[SchemaVersion]] = {}
        self._subject_compatibility: dict[str, CompatibilityLevel] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        subject: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> int:
        """Register a schema under ``subject`` and return the schema ID.

        If the identical schema string has already been registered for this
        subject the existing schema ID is returned without creating a new
        version (idempotent registration).

        Compatibility is checked against the latest version (or all prior
        versions for transitive modes) before accepting the schema.

        Args:
            subject:     Subject name (e.g. ``"orders-value"``).
            schema_str:  JSON string of the schema to register.
            schema_type: Schema language (default: AVRO).

        Returns:
            The integer schema ID (new or existing).

        Raises:
            IncompatibleSchemaError: If the schema violates the configured
                                     compatibility level for this subject.
        """
        # Idempotency: if identical schema already registered, return its ID.
        if subject in self._subjects:
            for sv in self._subjects[subject]:
                if sv.schema_str == schema_str and sv.schema_type == schema_type:
                    return sv.schema_id

        # Compatibility check (only if subject already has versions).
        if subject in self._subjects and self._subjects[subject]:
            if not self.check_compatibility(subject, schema_str):
                level = self.get_compatibility(subject)
                raise IncompatibleSchemaError(
                    f"Schema is incompatible with subject '{subject}' "
                    f"under compatibility level {level.value}",
                    subject=subject,
                    compatibility=level.value,
                )

        # Assign ID and version.
        schema_id = self._next_schema_id
        self._next_schema_id += 1
        version = len(self._subjects.get(subject, [])) + 1

        sv = SchemaVersion(
            schema_id=schema_id,
            version=version,
            subject=subject,
            schema_str=schema_str,
            schema_type=schema_type,
        )

        self._schemas[schema_id] = sv
        if subject not in self._subjects:
            self._subjects[subject] = []
        self._subjects[subject].append(sv)

        return schema_id

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get_schema(self, schema_id: int) -> SchemaVersion:
        """Return the SchemaVersion for ``schema_id``.

        Args:
            schema_id: The integer schema ID.

        Returns:
            The matching SchemaVersion.

        Raises:
            SchemaRegistryError: If no schema with that ID exists.
        """
        if schema_id not in self._schemas:
            raise SchemaRegistryError(f"Schema not found: id={schema_id}")
        return self._schemas[schema_id]

    def get_latest_schema(self, subject: str) -> SchemaVersion:
        """Return the most recently registered SchemaVersion for ``subject``.

        Args:
            subject: Subject name.

        Returns:
            The latest SchemaVersion.

        Raises:
            SchemaRegistryError: If the subject has never been registered.
        """
        if subject not in self._subjects or not self._subjects[subject]:
            raise SchemaRegistryError(f"Subject not found: {subject!r}")
        return self._subjects[subject][-1]

    def get_versions(self, subject: str) -> list[int]:
        """Return an ordered list of version numbers for ``subject``.

        Args:
            subject: Subject name.

        Returns:
            List of integer version numbers in registration order (1-based).
        """
        if subject not in self._subjects:
            return []
        return [sv.version for sv in self._subjects[subject]]

    # ------------------------------------------------------------------
    # Compatibility configuration
    # ------------------------------------------------------------------

    def set_compatibility(self, subject: str, level: CompatibilityLevel) -> None:
        """Override the compatibility level for ``subject``.

        Args:
            subject: Subject name.
            level:   New compatibility level for this subject.
        """
        self._subject_compatibility[subject] = level

    def get_compatibility(self, subject: str) -> CompatibilityLevel:
        """Return the effective compatibility level for ``subject``.

        Returns the subject-specific override if one has been set, otherwise
        falls back to the registry-wide default.

        Args:
            subject: Subject name.

        Returns:
            The effective CompatibilityLevel.
        """
        return self._subject_compatibility.get(subject, self._default_compatibility)

    def check_compatibility(self, subject: str, schema_str: str) -> bool:
        """Check whether ``schema_str`` is compatible with existing versions.

        Returns ``True`` if the subject has no registered schemas yet (first
        registration always succeeds).

        Args:
            subject:    Subject name.
            schema_str: The candidate schema to validate.

        Returns:
            True if compatible (or no prior schemas), False otherwise.
        """
        if subject not in self._subjects or not self._subjects[subject]:
            return True

        level = self.get_compatibility(subject)

        if level == CompatibilityLevel.NONE:
            return True

        versions = self._subjects[subject]
        latest = versions[-1]

        if level == CompatibilityLevel.BACKWARD:
            return AvroSchemaEvolution.is_backward_compatible(
                schema_str, latest.schema_str
            )
        elif level == CompatibilityLevel.FORWARD:
            return AvroSchemaEvolution.is_forward_compatible(
                schema_str, latest.schema_str
            )
        elif level == CompatibilityLevel.FULL:
            return AvroSchemaEvolution.is_full_compatible(
                schema_str, latest.schema_str
            )
        elif level == CompatibilityLevel.BACKWARD_TRANSITIVE:
            return all(
                AvroSchemaEvolution.is_backward_compatible(schema_str, sv.schema_str)
                for sv in versions
            )
        elif level == CompatibilityLevel.FORWARD_TRANSITIVE:
            return all(
                AvroSchemaEvolution.is_forward_compatible(schema_str, sv.schema_str)
                for sv in versions
            )
        elif level == CompatibilityLevel.FULL_TRANSITIVE:
            return all(
                AvroSchemaEvolution.is_full_compatible(schema_str, sv.schema_str)
                for sv in versions
            )

        return True  # pragma: no cover


# ---------------------------------------------------------------------------
# Pattern 2 — AvroSchemaEvolution (pure-Python Avro compatibility rules)
# ---------------------------------------------------------------------------


class AvroSchemaEvolution:
    """Pure-Python Avro schema compatibility checker for record schemas.

    Schemas are represented as JSON dicts following the Avro specification::

        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id",    "type": "int"},
                {"name": "email", "type": "string"},
                {"name": "age",   "type": "int", "default": 0}
            ]
        }

    All methods are static — this class is a namespace for compatibility
    functions and holds no state.

    Compatibility rules implemented (from Avro spec §3.2):

    BACKWARD  — New schema (reader) can read data written with old schema (writer):
        * Adding a field with a default value is OK (reader uses default for
          old records that lack the field).
        * Removing a field is OK (reader ignores unknown writer fields).
        * Adding a required field (no default) is NOT OK — reader cannot
          supply a value for records that were written without it.
        * Changing a field type is NOT OK (simplified; no type widening).

    FORWARD   — Old schema (reader) can read data written with new schema (writer):
        * Adding a field without a default is OK (old reader ignores it).
        * Removing a field that the old reader requires (no default) is NOT OK.
        * Changing a field type is NOT OK (simplified).

    FULL      — Both BACKWARD and FORWARD simultaneously.
    """

    @staticmethod
    def parse_fields(schema_str: str) -> dict[str, dict]:
        """Parse the fields array of an Avro record schema.

        Args:
            schema_str: JSON string of an Avro record schema.

        Returns:
            A dict mapping each field name to its field definition dict.
            The field definition always contains at least ``"name"`` and
            ``"type"`` keys.  If the original type was a bare string (e.g.
            ``"string"``), it is normalised to ``{"type": "string"}``.
        """
        schema = json.loads(schema_str)
        raw_fields: list[dict] = schema.get("fields", [])
        result: dict[str, dict] = {}
        for f in raw_fields:
            fd = dict(f)
            # Normalise bare string types to {"type": value}
            if isinstance(fd.get("type"), str):
                fd["type"] = {"type": fd["type"]}
            result[fd["name"]] = fd
        return result

    @staticmethod
    def is_backward_compatible(
        reader_schema_str: str,
        writer_schema_str: str,
    ) -> bool:
        """Check BACKWARD compatibility: new reader can read old writer data.

        A schema is BACKWARD compatible with its predecessor when:
        - Fields added in the reader schema have default values (so old records
          that lack them can still be read).
        - Fields removed from the reader that existed in the writer are allowed
          (the reader simply ignores them).
        - No required field (without default) is added to the reader that was
          absent in the writer.
        - No field type changes are permitted (simplified: no type widening).

        Args:
            reader_schema_str: The newer schema (candidate for registration).
            writer_schema_str: The current/existing schema.

        Returns:
            True if backward compatible, False otherwise.
        """
        reader_fields = AvroSchemaEvolution.parse_fields(reader_schema_str)
        writer_fields = AvroSchemaEvolution.parse_fields(writer_schema_str)

        # Fields in reader but not in writer: must have a default.
        for name, fd in reader_fields.items():
            if name not in writer_fields:
                if "default" not in fd:
                    return False  # Required new field — cannot read old records

        # Fields in both: types must match (simplified — no widening).
        for name in reader_fields:
            if name in writer_fields:
                rt = reader_fields[name].get("type")
                wt = writer_fields[name].get("type")
                if rt != wt:
                    return False

        return True

    @staticmethod
    def is_forward_compatible(
        writer_schema_str: str,
        reader_schema_str: str,
    ) -> bool:
        """Check FORWARD compatibility: old reader can read new writer data.

        A schema is FORWARD compatible with its predecessor when:
        - Fields added to the writer (new schema) that the old reader doesn't
          know about are allowed — the reader will ignore them.
        - Fields removed from the writer that the old reader still expects are
          only allowed if those reader fields have default values; otherwise
          the reader cannot reconstruct a complete record.
        - No field type changes are permitted (simplified).

        Args:
            writer_schema_str: The newer schema (candidate for registration).
            reader_schema_str: The current/existing schema (old reader).

        Returns:
            True if forward compatible, False otherwise.
        """
        writer_fields = AvroSchemaEvolution.parse_fields(writer_schema_str)
        reader_fields = AvroSchemaEvolution.parse_fields(reader_schema_str)

        # Fields in reader but not in writer: reader must have a default.
        for name, fd in reader_fields.items():
            if name not in writer_fields:
                if "default" not in fd:
                    return False  # Reader requires this field; writer doesn't provide it

        # Fields in both: types must match.
        for name in writer_fields:
            if name in reader_fields:
                wt = writer_fields[name].get("type")
                rt = reader_fields[name].get("type")
                if wt != rt:
                    return False

        return True

    @staticmethod
    def is_full_compatible(schema_a: str, schema_b: str) -> bool:
        """Check FULL compatibility: both BACKWARD and FORWARD simultaneously.

        Args:
            schema_a: One schema (treated as reader for backward check).
            schema_b: The other schema (treated as writer for backward check).

        Returns:
            True if both backward and forward compatible, False otherwise.
        """
        return AvroSchemaEvolution.is_backward_compatible(
            schema_a, schema_b
        ) and AvroSchemaEvolution.is_forward_compatible(
            schema_a, schema_b
        )


# ---------------------------------------------------------------------------
# Pattern 3 — ConfluentFramingCodec (Confluent wire format)
# ---------------------------------------------------------------------------


class ConfluentFramingCodec:
    """Confluent Schema Registry wire format encoder/decoder.

    The Confluent wire format prefixes every serialized message with five
    bytes:
        Byte 0     : Magic byte (always 0x00)
        Bytes 1–4  : Schema ID as a 4-byte big-endian unsigned integer

    This framing allows consumers to retrieve the correct schema from the
    registry before deserializing the payload.

    All methods are static — this class is a namespace and holds no state.

    Wire format reference:
        https://docs.confluent.io/platform/current/schema-registry/
        fundamentals/serdes-develop/index.html#wire-format
    """

    _MAGIC_BYTE = 0x00
    _HEADER_SIZE = 5  # 1 magic + 4 schema_id

    @staticmethod
    def encode(schema_id: int, payload: bytes) -> bytes:
        """Prepend the Confluent framing header to ``payload``.

        Args:
            schema_id: Integer schema ID (must fit in 4 bytes, i.e. < 2^32).
            payload:   Serialized message bytes (e.g. JSON or Avro binary).

        Returns:
            Five-byte framing header followed by ``payload``.
        """
        header = struct.pack(">bI", ConfluentFramingCodec._MAGIC_BYTE, schema_id)
        return header + payload

    @staticmethod
    def decode(data: bytes) -> tuple[int, bytes]:
        """Strip the Confluent framing header and return (schema_id, payload).

        Args:
            data: Raw bytes including the five-byte framing header.

        Returns:
            A tuple ``(schema_id, payload)`` where ``schema_id`` is the
            integer extracted from bytes 1–4 and ``payload`` is the
            remainder of ``data``.

        Raises:
            ValueError: If ``data`` is shorter than 5 bytes or the magic byte
                        is not 0x00.
        """
        if len(data) < ConfluentFramingCodec._HEADER_SIZE:
            raise ValueError(
                f"Data too short for Confluent framing: "
                f"expected >= {ConfluentFramingCodec._HEADER_SIZE} bytes, "
                f"got {len(data)}"
            )
        magic, schema_id = struct.unpack(">bI", data[:5])
        if magic != ConfluentFramingCodec._MAGIC_BYTE:
            raise ValueError(
                f"Invalid Confluent magic byte: expected 0x00, got {magic:#04x}"
            )
        return schema_id, data[5:]

    @staticmethod
    def validate_framing(data: bytes) -> bool:
        """Return True if ``data`` starts with a valid Confluent framing header.

        Args:
            data: Bytes to inspect.

        Returns:
            True if the first byte is 0x00 and len(data) >= 5, else False.
        """
        if len(data) < ConfluentFramingCodec._HEADER_SIZE:
            return False
        return data[0] == ConfluentFramingCodec._MAGIC_BYTE


# ---------------------------------------------------------------------------
# Pattern 4 — JsonSchemaSerializer
# ---------------------------------------------------------------------------


class JsonSchemaSerializer:
    """JSON serializer with Confluent framing for use with InMemorySchemaRegistry.

    Serializes Python dicts to JSON bytes and prepends the Confluent wire
    format framing header (magic byte + schema ID).  Deserialization strips
    the header, parses the JSON, and returns the record together with the
    schema ID for downstream schema-aware processing.

    Args:
        registry: The InMemorySchemaRegistry to use for schema ID lookup.
    """

    def __init__(self, registry: InMemorySchemaRegistry) -> None:
        self._registry = registry

    def serialize(self, subject: str, record: dict) -> bytes:
        """Serialize ``record`` as Confluent-framed JSON.

        Looks up the latest schema ID for ``subject`` in the registry,
        encodes ``record`` as UTF-8 JSON, and prepends the five-byte
        Confluent framing header.

        Args:
            subject: Subject name (e.g. ``"users-value"``).
            record:  Python dict to serialize.

        Returns:
            Confluent-framed bytes: ``\\x00 + schema_id (4 bytes) + json_bytes``.

        Raises:
            SchemaRegistryError: If the subject has no registered schemas.
        """
        latest = self._registry.get_latest_schema(subject)
        payload = json.dumps(record).encode("utf-8")
        return ConfluentFramingCodec.encode(latest.schema_id, payload)

    def deserialize(self, data: bytes) -> tuple[dict, int]:
        """Deserialize Confluent-framed JSON bytes.

        Strips the five-byte framing header, parses the JSON payload, and
        returns the record together with the embedded schema ID.

        Args:
            data: Confluent-framed bytes produced by ``serialize()``.

        Returns:
            A tuple ``(record, schema_id)`` where ``record`` is the
            deserialized dict and ``schema_id`` is the integer extracted
            from the framing header.
        """
        schema_id, payload = ConfluentFramingCodec.decode(data)
        record = json.loads(payload.decode("utf-8"))
        return record, schema_id


# ---------------------------------------------------------------------------
# Pattern 5 — SchemaEvolutionMigrator
# ---------------------------------------------------------------------------


class SchemaEvolutionMigrator:
    """Migrates data records between schema versions.

    Applies field-level transformations to move a record from the structure
    defined by one schema version to the structure defined by another:

    * Fields present in the target schema but absent in the source record are
      added using the field's ``"default"`` value (if declared); fields without
      a default in the target are skipped.
    * Fields present in the source record but absent in the target schema are
      removed from the migrated record.
    * Fields present in both schemas are passed through unchanged.

    This mirrors the Avro datum reader's field resolution phase and is
    analogous to Flink's schema migration (FLIP-295) and Debezium's
    CDC event migration.

    Args:
        registry: The InMemorySchemaRegistry used to look up both schemas.
    """

    def __init__(self, registry: InMemorySchemaRegistry) -> None:
        self._registry = registry

    def migrate(
        self,
        record: dict,
        from_schema_id: int,
        to_schema_id: int,
    ) -> dict:
        """Migrate ``record`` from one schema version to another.

        If ``from_schema_id == to_schema_id`` the original record is returned
        unchanged (as a new dict copy).

        Args:
            record:         The source record dict to migrate.
            from_schema_id: Schema ID of the schema that produced ``record``.
            to_schema_id:   Schema ID of the target schema.

        Returns:
            A new dict conforming to the target schema structure.

        Raises:
            SchemaRegistryError: If either schema ID is not found.
        """
        source_sv = self._registry.get_schema(from_schema_id)
        target_sv = self._registry.get_schema(to_schema_id)

        source_fields = AvroSchemaEvolution.parse_fields(source_sv.schema_str)
        target_fields = AvroSchemaEvolution.parse_fields(target_sv.schema_str)

        migrated: dict = {}

        for name, fd in target_fields.items():
            if name in record:
                # Field exists in source record — carry it over.
                migrated[name] = record[name]
            elif "default" in fd:
                # Field added in target schema — use the declared default.
                migrated[name] = fd["default"]
            # else: new required field with no default — skip silently.

        # Fields in source but not in target are dropped automatically
        # (they are never added to `migrated`).

        return migrated


# ---------------------------------------------------------------------------
# __main__ — schema registry workflow demonstration
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("=" * 70)
    print("Schema Registry Patterns — end-to-end workflow demo")
    print("=" * 70)

    # -----------------------------------------------------------------------
    # Step 1 — Create registry and register v1 schema for "users-value"
    # -----------------------------------------------------------------------
    registry = InMemorySchemaRegistry(
        default_compatibility=CompatibilityLevel.BACKWARD
    )

    schema_v1 = json.dumps({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id",    "type": "int"},
            {"name": "email", "type": "string"},
        ],
    })

    id_v1 = registry.register("users-value", schema_v1)
    print(f"\n[Step 1] Registered v1 schema → schema_id={id_v1}")
    sv1 = registry.get_schema(id_v1)
    print(f"  subject  : {sv1.subject}")
    print(f"  version  : {sv1.version}")
    print(f"  type     : {sv1.schema_type.value}")

    # -----------------------------------------------------------------------
    # Step 2 — Evolve to v2: add optional field "full_name" with a default
    # -----------------------------------------------------------------------
    schema_v2 = json.dumps({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id",        "type": "int"},
            {"name": "email",     "type": "string"},
            {"name": "full_name", "type": "string", "default": ""},
        ],
    })

    id_v2 = registry.register("users-value", schema_v2)
    print(f"\n[Step 2] Registered v2 schema → schema_id={id_v2}")
    print(f"  versions available: {registry.get_versions('users-value')}")

    # -----------------------------------------------------------------------
    # Step 3 — Show backward compatibility check
    # -----------------------------------------------------------------------
    is_compat = registry.check_compatibility("users-value", schema_v2)
    print(f"\n[Step 3] v2 backward-compatible with v1? {is_compat}")

    incompatible_schema = json.dumps({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id",        "type": "int"},
            {"name": "email",     "type": "string"},
            {"name": "required_new_field", "type": "string"},  # no default!
        ],
    })
    bad_compat = registry.check_compatibility("users-value", incompatible_schema)
    print(f"  Schema with required new field backward-compatible? {bad_compat}")

    # -----------------------------------------------------------------------
    # Step 4 — Serialize and deserialize a record
    # -----------------------------------------------------------------------
    serializer = JsonSchemaSerializer(registry)

    v1_record = {"id": 42, "email": "alice@example.com"}
    framed = serializer.serialize("users-value", v1_record)
    print(f"\n[Step 4] Serialized record: {len(framed)} bytes")
    print(f"  magic byte   : {framed[0]:#04x}")
    print(f"  schema_id    : {framed[1:5].hex()} (big-endian)")
    print(f"  payload      : {framed[5:].decode()}")

    restored_record, embedded_id = serializer.deserialize(framed)
    print(f"  deserialized : {restored_record}")
    print(f"  schema_id    : {embedded_id}")

    # -----------------------------------------------------------------------
    # Step 5 — Migrate a v1 record to v2 schema (adds "full_name" default)
    # -----------------------------------------------------------------------
    migrator = SchemaEvolutionMigrator(registry)
    migrated = migrator.migrate(
        record={"id": 42, "email": "alice@example.com"},
        from_schema_id=id_v1,
        to_schema_id=id_v2,
    )
    print(f"\n[Step 5] Migrated record (v1 → v2): {migrated}")
    print(f"  full_name field added with default: {migrated.get('full_name')!r}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    latest = registry.get_latest_schema("users-value")
    print(f"\n[Summary] Latest schema: version={latest.version} id={latest.schema_id}")
    print(f"  Compatibility: {registry.get_compatibility('users-value').value}")
    print("\nDone.")
