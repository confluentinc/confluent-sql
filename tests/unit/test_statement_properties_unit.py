"""Unit tests for the statement_properties enumerations (Issue #162)."""

import json
from datetime import timedelta

import pytest

import confluent_sql
from confluent_sql import InterfaceError
from confluent_sql.statement_properties import (
    Property,
    PropertyValue,
    ScanStartupMode,
    SnapshotMode,
    SnapshotWriteMode,
    StatementProperties,
    _to_flink_duration,
)

# Every sql.* statement key from the "Available SET options" table, verbatim off the wire.
# (The client.* options are gateway/client knobs, not statement properties, and are excluded.)
EXPECTED_PROPERTY_KEYS = {
    "sql.current-catalog",
    "sql.current-database",
    "sql.dry-run",
    "sql.inline-result",
    "sql.local-time-zone",
    "sql.snapshot.mode",
    "sql.snapshot.write-mode",
    "sql.state-ttl",
    "sql.tables.initial-offset-from",
    "sql.tables.scan.bounded.mode",
    "sql.tables.scan.bounded.timestamp-millis",
    "sql.tables.scan.idle-timeout",
    "sql.tables.scan.source-operator-parallelism",
    "sql.tables.scan.startup.mode",
    "sql.tables.scan.startup.specific-offsets",
    "sql.tables.scan.startup.timestamp-millis",
    "sql.tables.scan.watermark-alignment.max-allowed-drift",
}


@pytest.mark.unit
class TestProperty:
    """The Property key enum: exact wire strings and complete, drift-proof membership."""

    def test_property_key_set_is_exactly_the_documented_sql_options(self):
        """Property must name every sql.* SET option and no extras -- catches add/drop drift."""
        assert {member.value for member in Property} == EXPECTED_PROPERTY_KEYS

    @pytest.mark.parametrize(
        ("member", "wire_key"),
        [
            (Property.CURRENT_CATALOG, "sql.current-catalog"),
            (Property.CURRENT_DATABASE, "sql.current-database"),
            (Property.SNAPSHOT_MODE, "sql.snapshot.mode"),
            (Property.SNAPSHOT_WRITE_MODE, "sql.snapshot.write-mode"),
            (Property.STATE_TTL, "sql.state-ttl"),
            (Property.LOCAL_TIME_ZONE, "sql.local-time-zone"),
            (Property.SCAN_STARTUP_MODE, "sql.tables.scan.startup.mode"),
        ],
    )
    def test_property_member_pins_exact_wire_key(self, member, wire_key):
        """A Property member's value and its str-equality both equal the wire key exactly."""
        assert member.value == wire_key
        assert member == wire_key

    def test_property_member_is_a_plain_str(self):
        """Property members are str subclass instances, so they drop into PropertiesDict/JSON."""
        assert isinstance(Property.SNAPSHOT_WRITE_MODE, str)

    def test_property_member_stringifies_to_wire_key_not_python_name(self):
        """str()/f-string of a member yields the wire key, not 'Property.SNAPSHOT_MODE' --
        so a member interpolated into a log line or error message reads correctly."""
        assert str(Property.SNAPSHOT_MODE) == "sql.snapshot.mode"
        assert f"{Property.SNAPSHOT_MODE}" == "sql.snapshot.mode"


@pytest.mark.unit
class TestPropertyValues:
    """The value enums: exact wire values, a shared PropertyValue base, and bare-member JSON."""

    @pytest.mark.parametrize(
        ("member", "wire_value"),
        [
            (SnapshotWriteMode.DEFAULT, "default"),
            (SnapshotWriteMode.FAST_WRITE, "fast-write"),
            (SnapshotMode.NOW, "now"),
            (SnapshotMode.OFF, "off"),
        ],
    )
    def test_value_member_pins_exact_wire_value(self, member, wire_value):
        """Each value enum member's value and str-equality both equal the wire value exactly."""
        assert member.value == wire_value
        assert member == wire_value

    @pytest.mark.parametrize("value_enum", [SnapshotWriteMode, SnapshotMode])
    def test_value_enums_share_property_value_base(self, value_enum):
        """Every value enum derives from PropertyValue, the common statement-property-value base."""
        assert issubclass(value_enum, PropertyValue)

    def test_bare_members_json_serialize_to_wire_strings(self):
        """A dict keyed/valued by bare enum members serializes to the exact wire JSON --
        no .value unwrapping required by callers."""
        payload = {Property.SNAPSHOT_WRITE_MODE: SnapshotWriteMode.FAST_WRITE}
        assert json.loads(json.dumps(payload)) == {"sql.snapshot.write-mode": "fast-write"}


@pytest.mark.unit
class TestScanStartupMode:
    """The scan.startup.mode value enum -- Confluent's set, which is not Apache Flink's."""

    @pytest.mark.parametrize(
        ("member", "wire_value"),
        [
            (ScanStartupMode.EARLIEST_OFFSET, "earliest-offset"),
            (ScanStartupMode.LATEST_OFFSET, "latest-offset"),
            (ScanStartupMode.TIMESTAMP, "timestamp"),
            (ScanStartupMode.SPECIFIC_OFFSETS, "specific-offsets"),
        ],
    )
    def test_member_pins_exact_wire_value(self, member, wire_value):
        """Each member's value and str-equality both equal the wire value exactly."""
        assert member.value == wire_value
        assert member == wire_value

    def test_derives_from_property_value(self):
        assert issubclass(ScanStartupMode, PropertyValue)

    def test_omits_apache_flink_group_offsets(self):
        """Confluent's scan.startup.mode has no group-offsets (Apache Flink's default); guard
        against it creeping in from an Apache-Flink-shaped mental model."""
        assert "group-offsets" not in {member.value for member in ScanStartupMode}


@pytest.mark.unit
class TestToFlinkDuration:
    """timedelta -> Flink Duration string: whole seconds as `s`, else `ms`, else raise."""

    @pytest.mark.parametrize(
        ("delta", "wire"),
        [
            (timedelta(hours=1), "3600 s"),
            (timedelta(minutes=5), "300 s"),
            (timedelta(seconds=90), "90 s"),
            (timedelta(0), "0 s"),
            (timedelta(milliseconds=1500), "1500 ms"),
            (timedelta(milliseconds=250), "250 ms"),
        ],
    )
    def test_renders_expected_duration(self, delta, wire):
        assert _to_flink_duration(delta) == wire

    @pytest.mark.parametrize("delta", [timedelta(microseconds=1), timedelta(microseconds=500)])
    def test_sub_millisecond_raises(self, delta):
        """Flink Durations bottom out at ms; sub-ms precision would be silently lost, so raise."""
        with pytest.raises(ValueError, match="sub-millisecond"):
            _to_flink_duration(delta)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="negative"):
            _to_flink_duration(timedelta(seconds=-1))


@pytest.mark.unit
class TestStatementProperties:
    """The frozen typed view: rendering, forward-compat strings, and construction-time coherence."""

    def test_empty_renders_empty_dict(self):
        """An all-unset instance emits nothing -- never pins a default or fights the overlay."""
        assert StatementProperties().to_properties_dict() == {}

    def test_all_typed_fields_render_to_wire(self):
        sp = StatementProperties(
            snapshot_write_mode=SnapshotWriteMode.FAST_WRITE,
            state_ttl=timedelta(hours=1),
            scan_startup_mode=ScanStartupMode.LATEST_OFFSET,
            local_time_zone="America/Los_Angeles",
        )
        assert sp.to_properties_dict() == {
            "sql.snapshot.write-mode": "fast-write",
            "sql.state-ttl": "3600 s",
            "sql.tables.scan.startup.mode": "latest-offset",
            "sql.local-time-zone": "America/Los_Angeles",
        }

    def test_unset_fields_are_omitted(self):
        sp = StatementProperties(snapshot_write_mode=SnapshotWriteMode.DEFAULT)
        result = sp.to_properties_dict()
        assert result == {"sql.snapshot.write-mode": "default"}
        assert "sql.state-ttl" not in result

    def test_extra_passes_through_including_unmodeled_keys(self):
        sp = StatementProperties(extra={"sql.dry-run": True, "sql.some.future-option": "x"})
        assert sp.to_properties_dict() == {"sql.dry-run": True, "sql.some.future-option": "x"}

    def test_bare_string_value_reaches_wire_for_forward_compat(self):
        """A not-yet-modeled server value is passable as a raw str on the typed field, and lands
        verbatim -- to_properties_dict assigns the field directly, never `.value`."""
        result = StatementProperties(snapshot_write_mode="turbo-write").to_properties_dict()
        assert result == {"sql.snapshot.write-mode": "turbo-write"}

    def test_scan_startup_bare_string_reaches_wire(self):
        result = StatementProperties(scan_startup_mode="future-mode").to_properties_dict()
        assert result == {"sql.tables.scan.startup.mode": "future-mode"}

    @pytest.mark.parametrize(
        ("kwargs", "wrong_type", "expected_type"),
        [
            ({"snapshot_write_mode": SnapshotMode.NOW}, "SnapshotMode", "SnapshotWriteMode"),
            (
                {"scan_startup_mode": SnapshotWriteMode.FAST_WRITE},
                "SnapshotWriteMode",
                "ScanStartupMode",
            ),
        ],
    )
    def test_wrong_enum_member_raises(self, kwargs, wrong_type, expected_type):
        """A PropertyValue from the wrong property is a category error, rejected at construction."""
        field_name = next(iter(kwargs))
        with pytest.raises(
            InterfaceError,
            match=(
                rf"{field_name} was given a {wrong_type}; "
                rf"expected a {expected_type} or a raw str"
            ),
        ):
            StatementProperties(**kwargs)

    def test_correct_enum_and_bare_string_do_not_raise(self):
        StatementProperties(snapshot_write_mode=SnapshotWriteMode.FAST_WRITE)
        StatementProperties(snapshot_write_mode="anything-goes")
        StatementProperties(scan_startup_mode=ScanStartupMode.TIMESTAMP)
        StatementProperties(scan_startup_mode="anything-goes")

    def test_extra_colliding_with_set_field_raises(self):
        with pytest.raises(
            InterfaceError, match=r"extra may not contain 'sql\.snapshot\.write-mode'"
        ):
            StatementProperties(
                snapshot_write_mode=SnapshotWriteMode.FAST_WRITE,
                extra={"sql.snapshot.write-mode": "fast-write"},
            )

    def test_extra_colliding_with_unset_field_still_raises(self):
        """The collision rule keys off *modeled*, not *assigned*: the field is left unset here and
        the extra key is still rejected -- one spelling per property, always via the field."""
        with pytest.raises(InterfaceError, match=r"extra may not contain 'sql\.state-ttl'"):
            StatementProperties(extra={"sql.state-ttl": "3600 s"})

    def test_extra_collision_detected_when_keyed_by_enum_member(self):
        """A Property member used as an extra key collides just like its raw string would."""
        with pytest.raises(InterfaceError, match=r"extra may not contain 'sql\.state-ttl'"):
            StatementProperties(extra={Property.STATE_TTL: "3600 s"})


@pytest.mark.unit
def test_enums_are_reexported_from_package_root():
    """Property, PropertyValue, and the value enums are importable straight off confluent_sql."""
    assert confluent_sql.Property is Property
    assert confluent_sql.PropertyValue is PropertyValue
    assert confluent_sql.SnapshotWriteMode is SnapshotWriteMode
    assert confluent_sql.SnapshotMode is SnapshotMode
    assert confluent_sql.ScanStartupMode is ScanStartupMode
    assert confluent_sql.StatementProperties is StatementProperties
