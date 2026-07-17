"""Discoverable, type-checkable enumerations of Flink SQL statement properties.

Statement properties are the `SET` options a statement is submitted with (`spec.properties` on the
wire). Callers can pass raw `str` keys and values, but this module curates the ones worth
discovering: the property *keys* as `Property`, and the fixed value sets as `PropertyValue`
subclasses. See the "Available SET options" reference:
https://docs.confluent.io/cloud/current/flink/reference/statements/set.html

Both `Property` and `PropertyValue` subclass `_PropertyEnum` -- a `(str, Enum)` with `StrEnum`
string semantics -- rather than `enum.StrEnum`, because the repo floor is Python 3.10 and `StrEnum`
is 3.11+ (`(str, Enum)` is also the house style: `TableFormat`, `ConnectorState`, `TableflowPhase`).
Members are genuine `str` instances: they compare and hash as their wire string, so they drop
straight into `PropertiesDict`, satisfy the `isinstance(v, (str, int, bool))` validation in
`Connection._resolve_properties`, JSON-serialize to the bare wire string, and (via `_PropertyEnum`)
stringify to it in logs and error messages too -- all with no `.value` unwrapping at the call site.
"""

from enum import Enum


class _PropertyEnum(str, Enum):
    """`(str, Enum)` with `enum.StrEnum` string semantics, for the 3.10 floor (StrEnum is 3.11+).

    Plain `(str, Enum)` renders `str(member)`/`f"{member}"` as `"Class.MEMBER"`, so a member
    interpolated into a log line or error message would leak the Python name instead of the wire
    string. Delegating `__str__`/`__format__` to `str` -- exactly what CPython's `StrEnum` does --
    makes a member stringify to its wire value everywhere, not just under `json.dumps`.
    """

    __str__ = str.__str__
    __format__ = str.__format__


class Property(_PropertyEnum):
    """A Flink SQL statement property key -- the `sql.*` options from the SET-options reference.

    Only statement-scoped `sql.*` keys are modeled; the `client.*` options from the same reference
    are gateway/client knobs, not statement properties, so they are deliberately omitted.
    """

    CURRENT_CATALOG = "sql.current-catalog"
    CURRENT_DATABASE = "sql.current-database"
    DRY_RUN = "sql.dry-run"
    INLINE_RESULT = "sql.inline-result"
    LOCAL_TIME_ZONE = "sql.local-time-zone"
    SNAPSHOT_MODE = "sql.snapshot.mode"
    SNAPSHOT_WRITE_MODE = "sql.snapshot.write-mode"
    STATE_TTL = "sql.state-ttl"
    INITIAL_OFFSET_FROM = "sql.tables.initial-offset-from"
    SCAN_BOUNDED_MODE = "sql.tables.scan.bounded.mode"
    SCAN_BOUNDED_TIMESTAMP_MILLIS = "sql.tables.scan.bounded.timestamp-millis"
    SCAN_IDLE_TIMEOUT = "sql.tables.scan.idle-timeout"
    SCAN_SOURCE_OPERATOR_PARALLELISM = "sql.tables.scan.source-operator-parallelism"
    SCAN_STARTUP_MODE = "sql.tables.scan.startup.mode"
    SCAN_STARTUP_SPECIFIC_OFFSETS = "sql.tables.scan.startup.specific-offsets"
    SCAN_STARTUP_TIMESTAMP_MILLIS = "sql.tables.scan.startup.timestamp-millis"
    SCAN_WATERMARK_ALIGNMENT_MAX_ALLOWED_DRIFT = (
        "sql.tables.scan.watermark-alignment.max-allowed-drift"
    )


class PropertyValue(_PropertyEnum):
    """Common base for enums of a property's fixed, enumerated value set.

    Gives every such value enum one shared identity to test against and lets `StatementProperties`
    (#163) treat them uniformly. Only properties with a documented fixed value set get a subclass;
    free-form values (catalog names, time zones, durations) stay plain.
    """


class SnapshotWriteMode(PropertyValue):
    """Values for `Property.SNAPSHOT_WRITE_MODE` -- the write mode for snapshot queries."""

    DEFAULT = "default"
    FAST_WRITE = "fast-write"  # disables exactly-once for performance; snapshot mode only


class SnapshotMode(PropertyValue):
    """Values for `Property.SNAPSHOT_MODE` -- whether a statement runs as a snapshot query."""

    NOW = "now"
    OFF = "off"
