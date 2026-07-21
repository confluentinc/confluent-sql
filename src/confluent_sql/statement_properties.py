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

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar

from .exceptions import InterfaceError

if TYPE_CHECKING:
    # Imported for annotations only: types.py imports the enums below, so a runtime import here
    # would close a cycle. `from __future__ import annotations` keeps the references lazy.
    from .types import PropertiesDict


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


class ScanStartupMode(PropertyValue):
    """Values for `Property.SCAN_STARTUP_MODE` -- where Confluent-native table scans begin.

    This is Confluent's set, which is deliberately *not* Apache Flink's: Flink additionally offers
    `group-offsets` and defaults to it, whereas Confluent omits it and defaults to
    `earliest-offset`. `timestamp` and `specific-offsets` each require their companion option
    (`sql.tables.scan.startup.timestamp-millis` / `sql.tables.scan.startup.specific-offsets`) to be
    set as well.
    """

    EARLIEST_OFFSET = "earliest-offset"
    LATEST_OFFSET = "latest-offset"
    TIMESTAMP = "timestamp"
    SPECIFIC_OFFSETS = "specific-offsets"


def _to_flink_duration(delta: timedelta) -> str:
    """Render a `timedelta` to a Flink `Duration` string, e.g. `timedelta(hours=1)` -> `"3600 s"`.

    Whole seconds print as `"<n> s"`, otherwise `"<n> ms"`. Flink Durations bottom out at
    millisecond granularity, so a sub-millisecond `timedelta` would silently lose precision -- that
    raises instead, as does a negative duration (meaningless for the TTL this serves).
    """
    total_us = delta // timedelta(microseconds=1)
    if total_us < 0:
        raise ValueError(f"duration must be non-negative, got negative {delta!r}")
    if total_us % 1000 != 0:
        raise ValueError(f"duration has sub-millisecond precision Flink cannot express: {delta!r}")
    total_ms = total_us // 1000
    if total_ms % 1000 == 0:
        return f"{total_ms // 1000} s"
    return f"{total_ms} ms"


@dataclass(frozen=True, kw_only=True)
class StatementProperties:
    """Typed, validated view over a curated subset of Flink SET options.

    Unset fields (None) are omitted entirely; only explicitly set options are emitted, so this
    never fights the driver-owned overlay (catalog/database/snapshot.mode) or pins a server default.
    An `extra` dict is the escape hatch for options this class does not yet model -- and *only* for
    those: putting a modeled option's key in `extra` raises, so each property has exactly one
    spelling.

    The enum-typed fields also accept a raw `str` for forward compatibility: a Flink-side value this
    driver doesn't model yet can be passed directly on the field rather than smuggled through
    `extra`. A `PropertyValue` from the *wrong* property (e.g. a `SnapshotMode` on
    `snapshot_write_mode`) is a category error and is rejected at construction.

    Field definitions and defaults track the Flink "Available SET options" reference:
    https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#available-set-options
    """

    snapshot_write_mode: SnapshotWriteMode | str | None = None
    """`sql.snapshot.write-mode` (server default `"default"`): write mode for snapshot queries.
    `"fast-write"` trades exactly-once delivery for speed. Valid only in snapshot mode."""

    state_ttl: timedelta | None = None
    """`sql.state-ttl` (server default 0 ms, meaning no expiry): the minimum time idle state --
    state that hasn't been updated -- is retained before it becomes eligible for clearance (the
    system decides the actual clearance moment after this interval). Rendered to a Flink duration
    string."""

    scan_startup_mode: ScanStartupMode | str | None = None
    """`sql.tables.scan.startup.mode` (server default: the table's own): overwrites
    `scan.startup.mode` for Confluent-native tables in newly created queries. Not applied to a table
    that already uses a non-default value."""

    local_time_zone: str | None = None
    """`sql.local-time-zone` (server default `"UTC"`): the time zone used when converting
    `TIMESTAMP_LTZ` to types without a zone (`TIMESTAMP`, `TIME`, or `STRING`). A TZDB id like
    `"America/Los_Angeles"` or a fixed offset like `"GMT+03:00"`."""

    extra: PropertiesDict = field(default_factory=dict)
    """Escape hatch for SET options not modeled as a typed field above -- raw wire keys to values.
    May not carry a key a typed field already models; doing so raises at construction. The dict is
    copied into a read-only mapping at construction, so the instance stays as frozen as its
    fields."""

    _MODELED_KEY_TO_FIELD: ClassVar[dict[Property, str]] = {
        Property.SNAPSHOT_WRITE_MODE: "snapshot_write_mode",
        Property.STATE_TTL: "state_ttl",
        Property.SCAN_STARTUP_MODE: "scan_startup_mode",
        Property.LOCAL_TIME_ZONE: "local_time_zone",
    }
    """Every property this class models as a typed field, mapped to that field's name -- the source
    of truth for both rejecting `extra` collisions and naming the field to use instead."""

    def __post_init__(self) -> None:
        self._reject_wrong_enum("snapshot_write_mode", self.snapshot_write_mode, SnapshotWriteMode)
        self._reject_wrong_enum("scan_startup_mode", self.scan_startup_mode, ScanStartupMode)
        self._reject_wrong_field_types()
        self._reject_extra_collisions()
        # Snapshot extra into a read-only mapping so the collision check above can't be voided by
        # mutating extra after construction. object.__setattr__ is the frozen escape hatch.
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))

    @staticmethod
    def _reject_wrong_enum(field_name: str, value: object, expected: type[PropertyValue]) -> None:
        """Reject a `PropertyValue` belonging to a different property; a raw str always passes."""
        if isinstance(value, PropertyValue) and not isinstance(value, expected):
            raise InterfaceError(
                f"{field_name} was given a {type(value).__name__}; "
                f"expected a {expected.__name__} or a raw str"
            )

    def _reject_wrong_field_types(self) -> None:
        """Reject a field given the wrong Python type, so a bad value surfaces as a consistent
        InterfaceError here rather than a TypeError deep in rendering (e.g. a str `state_ttl`
        reaching `_to_flink_duration`). Enum-typed fields accept their bare `str` too."""
        self._reject_wrong_type(
            "snapshot_write_mode", self.snapshot_write_mode, str,
            "a SnapshotWriteMode, a raw str, or None",
        )
        self._reject_wrong_type(
            "scan_startup_mode", self.scan_startup_mode, str,
            "a ScanStartupMode, a raw str, or None",
        )
        self._reject_wrong_type("state_ttl", self.state_ttl, timedelta, "a timedelta or None")
        self._reject_wrong_type("local_time_zone", self.local_time_zone, str, "a str or None")
        if not isinstance(self.extra, dict):
            raise InterfaceError(f"extra must be a dict, got {type(self.extra).__name__}")

    @staticmethod
    def _reject_wrong_type(
        field_name: str, value: object, expected: type, description: str
    ) -> None:
        """Reject a non-None field value that isn't an instance of `expected`; None is always ok."""
        if value is not None and not isinstance(value, expected):
            raise InterfaceError(
                f"{field_name} must be {description}, got {type(value).__name__}"
            )

    def _reject_extra_collisions(self) -> None:
        """Reject any `extra` key a typed field already models -- one spelling per property."""
        for key in self.extra:
            field_name = self._MODELED_KEY_TO_FIELD.get(key)  # type: ignore[arg-type]
            if field_name is not None:
                raise InterfaceError(
                    f"extra may not contain '{key}'; set it via the {field_name} field instead"
                )

    def to_properties_dict(self) -> PropertiesDict:
        """Render to the wire properties dict: `extra` first, then each set typed field on top.

        Typed values are assigned directly, not via `.value`, so an enum member and a raw
        forward-compat string both serialize correctly (`_PropertyEnum` members are wire strings).
        """
        props: PropertiesDict = dict(self.extra)
        if self.snapshot_write_mode is not None:
            props[Property.SNAPSHOT_WRITE_MODE] = self.snapshot_write_mode
        if self.state_ttl is not None:
            props[Property.STATE_TTL] = _to_flink_duration(self.state_ttl)
        if self.scan_startup_mode is not None:
            props[Property.SCAN_STARTUP_MODE] = self.scan_startup_mode
        if self.local_time_zone is not None:
            props[Property.LOCAL_TIME_ZONE] = self.local_time_zone
        return props
