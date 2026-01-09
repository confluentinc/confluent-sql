from collections.abc import Callable
from typing import Any, TypeAlias

import pytest

from confluent_sql import DatabaseError, OperationalError
from confluent_sql.exceptions import InterfaceError
from confluent_sql.statement import Op, Phase, Schema, Statement
from confluent_sql.types import StatementTypeConverter
from tests.unit.conftest import StatementResponseFactory

"""Unit tests over Statement class."""

# Stop complaining about inlined constants. These are test cases!
# ruff: noqa: PLR2004


@pytest.mark.unit
class TestOp:
    @pytest.mark.parametrize(
        "op,expected_str",
        [(Op.INSERT, "+I"), (Op.UPDATE_BEFORE, "-U"), (Op.UPDATE_AFTER, "+U"), (Op.DELETE, "-D")],
    )
    def test_str(self, op: Op, expected_str: str):
        assert str(op) == expected_str


@pytest.mark.unit
class TestStatementIsReady:
    """Tests for Statement.is_ready property."""

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED"])
    def test_bounded_is_ready(self, statement_response_factory: StatementResponseFactory, phase):
        """Test that a bounded statement in COMPLETED or
        STOPPED phase is ready."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
        )
        statement = Statement.from_response(statement_json)
        assert statement.is_ready

    def test_bounded_not_ready(self, statement_response_factory: StatementResponseFactory):
        """Test that a bounded statement not in COMPLETED or
        STOPPED phase is not ready."""
        statement_json = statement_response_factory(
            phase="RUNNING",
            is_bounded=True,
        )
        statement = Statement.from_response(statement_json)
        assert not statement.is_ready

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "RUNNING"])
    def test_unbounded_is_ready(
        self, statement_response_factory: StatementResponseFactory, phase: str
    ):
        """Test that an unbounded statement in COMPLETED, STOPPED,
        or RUNNING phase is ready."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=False,
        )
        statement = Statement.from_response(statement_json)
        assert statement.is_ready

    def test_unbounded_pending_not_ready(
        self, statement_response_factory: StatementResponseFactory
    ):
        """Test that an unbounded statement not in PENDING phase is not ready."""
        statement_json = statement_response_factory(
            phase="PENDING",
            is_bounded=False,
        )
        statement = Statement.from_response(statement_json)
        assert not statement.is_ready


@pytest.mark.unit
class TestStatementDescriptionProperty:
    """Tests for Statement.description property."""

    def test_description_present(self, statement_response_factory: StatementResponseFactory):
        """Test that description property returns correct value when present."""
        statement_json = statement_response_factory(
            schema_columns=[
                {"name": "str_col", "type": {"type": "VARCHAR", "nullable": False}},
                {
                    "name": "dec_col",
                    "type": {"type": "DEC", "nullable": True, "precision": 10, "scale": 2},
                },
            ]
        )
        statement = Statement.from_response(statement_json)

        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        assert statement.description == [
            ("str_col", "VARCHAR", None, None, None, None, False),
            ("dec_col", "DEC", None, None, 10, 2, True),
        ]

    def test_description_absent(self, statement_response_factory: StatementResponseFactory):
        """Test that description property returns None when no schema(yet)."""
        statement_json = statement_response_factory(null_schema=True)
        statement = Statement.from_response(statement_json)
        assert statement.description is None


@pytest.mark.unit
class TestStatementProperties:
    """Tests for various Statement properties."""

    def test_compute_pool_id(self, statement_response_factory: StatementResponseFactory):
        """Test that compute_pool_id property returns correct value."""
        statement_json = statement_response_factory(compute_pool_id="test-pool-id")
        statement = Statement.from_response(statement_json)
        assert statement.compute_pool_id == "test-pool-id"

    def test_principal(self, statement_response_factory: StatementResponseFactory):
        """Test that principal property returns correct value."""
        statement_json = statement_response_factory(principal="test-principal")
        statement = Statement.from_response(statement_json)
        assert statement.principal == "test-principal"

    def test_phase_property(self, statement_response_factory: StatementResponseFactory):
        """Test that phase property returns correct Phase enum."""
        statement_json = statement_response_factory(phase="RUNNING")
        statement = Statement.from_response(statement_json)
        assert statement.phase == Phase.RUNNING

    def test_phase_when_deleted(self, statement_response_factory: StatementResponseFactory):
        """Test that phase property returns DELETED when statement is deleted."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(statement_json)
        # Simulate deletion
        statement.set_deleted()
        assert statement.phase == Phase.DELETED

    @pytest.mark.parametrize(
        "phase,expected",
        [
            ("RUNNING", True),
            ("COMPLETED", False),
            ("STOPPED", False),
        ],
    )
    def test_is_running(
        self, statement_response_factory: StatementResponseFactory, phase: str, expected: bool
    ):
        """Test that is_running property returns correct boolean."""
        statement_json = statement_response_factory(phase=phase)
        statement = Statement.from_response(statement_json)
        assert statement.is_running == expected

    def test_type_converter_raises_if_no_schema(
        self, statement_response_factory: StatementResponseFactory
    ):
        """Test that type_converter property raises if statement has no schema."""
        # Create a statement response with no schema.
        statement_json = statement_response_factory(null_schema=True)
        statement = Statement.from_response(statement_json)

        with pytest.raises(
            InterfaceError,
            match="Cannot get type converter for statement with no schema.",
        ):
            _ = statement.type_converter

    def test_type_converter_returns_converter(
        self,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that type_converter property returns a StatementTypeConverter
        when schema is present."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(statement_json)

        type_converter = statement.type_converter
        assert isinstance(type_converter, StatementTypeConverter)


@pytest.mark.unit
class TestStatementFromResponse:
    """Tests for Statement.from_response class method error paths."""

    def test_hates_unknown_status_phase(self, statement_response_factory: StatementResponseFactory):
        """Test that from_response raises on unknown status.phase."""
        with pytest.raises(OperationalError, match="Received an unknown phase for statement"):
            Statement.from_response(statement_response_factory(phase="UNKNOWN"))

    def test_raises_databaseerror_if_failed(
        self, statement_response_factory: StatementResponseFactory
    ):
        """Test that a failed statement raises DatabaseError with details when if the
        statement failed."""
        failed_statement_json = statement_response_factory(
            phase="FAILED", status_detail="Some error"
        )
        with pytest.raises(DatabaseError, match="Some error"):
            Statement.from_response(failed_statement_json)

    def test_hates_missing_keys(self, statement_response_factory: StatementResponseFactory):
        """Test that from_response raises if required keys are missing."""
        incomplete_json = statement_response_factory()
        del incomplete_json["spec"]

        with pytest.raises(
            OperationalError, match="Error parsing statement response, missing 'spec'"
        ):
            Statement.from_response(incomplete_json)

    def test_parses_row_result_schema(self, statement_response_factory: StatementResponseFactory):
        """Test that from_response correctly parses a statement response with
        a schema that includes a two-member ROW type column."""
        statement_json = statement_response_factory(
            schema_columns=[
                {
                    "name": "simple_row",
                    "type": {
                        "fields": [
                            {
                                "field_type": {"length": 5, "nullable": False, "type": "CHAR"},
                                "name": "EXPR$0",
                            },
                            {
                                "field_type": {"nullable": False, "type": "INTEGER"},
                                "name": "EXPR$1",
                            },
                        ],
                        "nullable": False,
                        "type": "ROW",
                    },
                }
            ]
        )
        statement = Statement.from_response(statement_json)
        assert statement.schema is not None
        assert len(statement.schema.columns) == 1
        row_column = statement.schema.columns[0]
        assert row_column.name == "simple_row"
        assert row_column.type.type == "ROW"
        assert row_column.type.fields is not None
        assert len(row_column.type.fields) == 2
        field0 = row_column.type.fields[0]
        assert field0.name == "EXPR$0"
        assert field0.field_type.type == "CHAR"
        assert field0.field_type.length == 5
        assert field0.field_type.nullable is False
        field1 = row_column.type.fields[1]
        assert field1.name == "EXPR$1"
        assert field1.field_type.type == "INTEGER"
        assert field1.field_type.nullable is False


@pytest.mark.unit
class TestSchemaParsing:
    """Tests for parsing Schema from Statement responses with realistic column type descriptions."""

    nullable_field_values = [True, False]

    SchemaFactory: TypeAlias = Callable[[list[dict[str, Any]]], Schema]

    @pytest.fixture()
    def schema_factory(self, statement_response_factory: StatementResponseFactory) -> SchemaFactory:
        def _schema_maker(
            schema_columns: list[dict[str, Any]],
        ) -> Schema:
            statement_dict = statement_response_factory(
                schema_columns=schema_columns,
            )
            statement = Statement.from_response(statement_dict)
            assert statement.schema is not None
            return statement.schema

        return _schema_maker

    @pytest.mark.parametrize("nullable", nullable_field_values)
    def test_int(self, schema_factory: SchemaFactory, nullable: bool):
        schema = schema_factory(
            [{"name": "id", "type": {"type": "INT", "nullable": nullable}}],
        )
        assert len(schema.columns) == 1
        id_column = schema.columns[0]
        assert id_column.name == "id"
        assert id_column.type.type == "INT"
        assert id_column.type.nullable == nullable

    @pytest.mark.parametrize("nullable", nullable_field_values)
    def test_string(self, schema_factory: SchemaFactory, nullable: bool):
        schema = schema_factory(
            [{"name": "name", "type": {"type": "STRING", "length": 100, "nullable": nullable}}],
        )
        assert len(schema.columns) == 1
        name_column = schema.columns[0]
        assert name_column.name == "name"
        assert name_column.type.type == "STRING"
        assert name_column.type.length == 100
        assert name_column.type.nullable == nullable

    @pytest.mark.parametrize("nullable", nullable_field_values)
    def test_decimal(self, schema_factory: SchemaFactory, nullable: bool):
        schema = schema_factory(
            [
                {
                    "name": "price",
                    "type": {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": nullable},
                }
            ],
        )
        assert len(schema.columns) == 1
        price_column = schema.columns[0]
        assert price_column.name == "price"
        assert price_column.type.type == "DECIMAL"
        assert price_column.type.precision == 10
        assert price_column.type.scale == 2
        assert price_column.type.nullable == nullable

    @pytest.mark.parametrize("nullable", nullable_field_values)
    def test_interval_day_to_second(self, schema_factory: SchemaFactory, nullable: bool):
        schema = schema_factory(
            [
                {
                    "name": "duration",
                    "type": {
                        "type": "INTERVAL_DAY_TO_SECOND",
                        "fractional_precision": 3,
                        "resolution": "DAY_TO_SECOND",
                        "nullable": nullable,
                    },
                }
            ],
        )
        assert len(schema.columns) == 1
        duration_column = schema.columns[0]
        assert duration_column.name == "duration"
        assert duration_column.type.type == "INTERVAL_DAY_TO_SECOND"
        assert duration_column.type.fractional_precision == 3
        assert duration_column.type.resolution == "DAY_TO_SECOND"
        assert duration_column.type.nullable == nullable

    @pytest.mark.parametrize("nullable", nullable_field_values)
    def test_row_type(self, schema_factory: SchemaFactory, nullable: bool):
        schema = schema_factory(
            [
                {
                    "name": "details",
                    "type": {
                        "type": "ROW",
                        "fields": [
                            {
                                "name": "age",
                                "field_type": {"type": "INT", "nullable": True},
                                "description": "Age in years",
                            },
                            {
                                "name": "address",
                                "field_type": {"type": "STRING", "length": 100, "nullable": True},
                                "description": "Residential address",
                            },
                        ],
                        "nullable": nullable,
                    },
                }
            ],
        )
        assert len(schema.columns) == 1
        details_column = schema.columns[0]
        assert details_column.name == "details"
        assert details_column.type.type == "ROW"
        assert details_column.type.nullable == nullable
        assert details_column.type.fields is not None
        assert len(details_column.type.fields) == 2

        age_field = details_column.type.fields[0]
        assert age_field.name == "age"
        assert age_field.field_type.type == age_field.type.type == "INT"
        assert age_field.type.nullable is True
        assert age_field.description == "Age in years"

        address_field = details_column.type.fields[1]
        assert address_field.name == "address"
        assert address_field.field_type.type == address_field.type.type == "STRING"
        assert address_field.type.length == 100
        assert address_field.type.nullable is True
        assert address_field.description == "Residential address"

    def test_multiple_columns(self, schema_factory: SchemaFactory):
        schema = schema_factory(
            [
                {"name": "id", "type": {"type": "INT", "nullable": False}},
                {"name": "name", "type": {"type": "STRING", "length": 50, "nullable": True}},
            ],
        )
        assert len(schema.columns) == 2
        assert [col.name for col in schema.columns] == ["id", "name"]

        # Don't bother about the rest, let the individual column tests handle that.
