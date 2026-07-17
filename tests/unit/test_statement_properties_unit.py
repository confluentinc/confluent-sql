"""Unit tests for the statement_properties enumerations (Issue #162)."""

import json

import pytest

import confluent_sql
from confluent_sql.statement_properties import (
    Property,
    PropertyValue,
    SnapshotMode,
    SnapshotWriteMode,
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
        payload = {Property.STATE_TTL: SnapshotWriteMode.FAST_WRITE}
        assert json.loads(json.dumps(payload)) == {"sql.state-ttl": "fast-write"}


@pytest.mark.unit
def test_enums_are_reexported_from_package_root():
    """Property, PropertyValue, and the value enums are importable straight off confluent_sql."""
    assert confluent_sql.Property is Property
    assert confluent_sql.PropertyValue is PropertyValue
    assert confluent_sql.SnapshotWriteMode is SnapshotWriteMode
    assert confluent_sql.SnapshotMode is SnapshotMode
