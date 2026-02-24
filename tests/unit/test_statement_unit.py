from collections.abc import Callable
from typing import Any, TypeAlias

import pytest

from confluent_sql import OperationalError
from confluent_sql.connection import Connection
from confluent_sql.exceptions import InterfaceError
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import Op, Phase, Schema, Statement
from confluent_sql.types import StatementTypeConverter
from tests.unit.conftest import StatementResponseFactory

"""Unit tests over Statement class."""


@pytest.mark.unit
class TestOp:
    @pytest.mark.parametrize(
        "op,expected_str",
        [(Op.INSERT, "+I"), (Op.UPDATE_BEFORE, "-U"), (Op.UPDATE_AFTER, "+U"), (Op.DELETE, "-D")],
    )
    def test_str(self, op: Op, expected_str: str):
        assert str(op) == expected_str


@pytest.mark.unit
class TestPhaseIsTerminal:
    """Tests for Phase.is_terminal property."""

    @pytest.mark.parametrize(
        "phase,expected",
        [
            (Phase.COMPLETED, True),
            (Phase.STOPPED, True),
            (Phase.FAILED, True),
            (Phase.DELETED, True),
            (Phase.RUNNING, False),
            (Phase.PENDING, False),
            (Phase.DEGRADED, False),
            (Phase.STOPPING, False),
        ],
    )
    def test_is_terminal(self, phase: Phase, expected: bool):
        """Test that is_terminal property returns correct boolean for each phase."""
        assert phase.is_terminal == expected


@pytest.mark.unit
class TestStatementDescriptionProperty:
    """Tests for Statement.description property."""

    def test_description_present(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
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
        statement = Statement.from_response(mock_connection, statement_json)

        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        assert statement.description == [
            ("str_col", "VARCHAR", None, None, None, None, False),
            ("dec_col", "DEC", None, None, 10, 2, True),
        ]

    def test_description_absent(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that description property returns None when no schema(yet)."""
        statement_json = statement_response_factory(null_schema=True)
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.description is None


@pytest.mark.unit
class TestStatementProperties:
    """Tests for various Statement properties."""

    def test_compute_pool_id(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that compute_pool_id property returns correct value."""
        statement_json = statement_response_factory(compute_pool_id="test-pool-id")
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.compute_pool_id == "test-pool-id"

    def test_principal(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that principal property returns correct value."""
        statement_json = statement_response_factory(principal="test-principal")
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.principal == "test-principal"

    def test_phase_property(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that phase property returns correct Phase enum."""
        statement_json = statement_response_factory(phase="RUNNING")
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.phase == Phase.RUNNING

    def test_phase_when_deleted(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that phase property returns DELETED when statement is deleted."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(mock_connection, statement_json)
        # Simulate client-side deletion
        statement.set_deleted()
        assert statement.phase == Phase.DELETED
        assert statement.is_deleted

    @pytest.mark.parametrize(
        "phase,expected",
        [
            ("RUNNING", True),
            ("COMPLETED", False),
            ("STOPPED", False),
        ],
    )
    def test_is_running(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
        expected: bool,
    ):
        """Test that is_running property returns correct boolean."""
        statement_json = statement_response_factory(phase=phase)
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.is_running == expected

    def test_type_converter_raises_if_no_schema(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that type_converter property raises if statement has no schema."""
        # Create a statement response with no schema.
        statement_json = statement_response_factory(null_schema=True)
        statement = Statement.from_response(mock_connection, statement_json)

        with pytest.raises(
            InterfaceError,
            match="Cannot get type converter for statement with no schema.",
        ):
            _ = statement.type_converter

    def test_type_converter_returns_converter(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that type_converter property returns a StatementTypeConverter
        when schema is present."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(mock_connection, statement_json)

        type_converter = statement.type_converter
        assert isinstance(type_converter, StatementTypeConverter)

    @pytest.mark.parametrize(
        "phase,expected",
        [
            (Phase.COMPLETED, True),
            (Phase.FAILED, True),
            (Phase.STOPPED, True),
            (Phase.RUNNING, False),
            (Phase.PENDING, False),
            (Phase.DEGRADED, False),
            (Phase.DELETED, False),
        ],
    )
    def test_statement_is_deletable(
        self,
        phase: Phase,
        expected: bool,
        mock_connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test Statement.is_deletable property."""
        statement = Statement.from_response(
            mock_connection,
            statement_response_factory(phase=phase.name),
        )
        assert statement.is_deletable == expected

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "phase,expected",
        [
            (Phase.COMPLETED, False),
            (Phase.FAILED, False),
            (Phase.STOPPED, False),
            (Phase.RUNNING, False),
            (Phase.PENDING, False),
            (Phase.DEGRADED, True),
            (Phase.DELETED, False),
        ],
    )
    def test_statement_is_degraded(
        self,
        phase: Phase,
        expected: bool,
        mock_connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test Statement.is_degraded property."""
        statement = Statement.from_response(
            mock_connection,
            statement_response_factory(phase=phase.name),
        )
        assert statement.is_degraded == expected

    @pytest.mark.parametrize(
        "is_append_only",
        [True, False],
    )
    def test_is_append_only_property(
        self,
        is_append_only: bool,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_append_only property returns correct value."""
        statement_json = statement_response_factory(is_append_only=is_append_only)
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.is_append_only is is_append_only

    def test_is_append_only_raises_when_no_traits(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_append_only property raises InterfaceError when
        failed statement traits are missing."""

        statement_json = statement_response_factory(phase="FAILED")
        statement = Statement.from_response(mock_connection, statement_json)

        with pytest.raises(
            InterfaceError,
            match="Statement traits are not available.",
        ):
            _ = statement.is_append_only

    def test_is_deleted_property(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_deleted property returns correct value."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.is_deleted is False

        # Simulate deletion
        statement.set_deleted()
        assert statement.is_deleted is True

    @pytest.mark.parametrize(
        "sql_kind",
        [
            "SELECT",
            "INSERT",
        ],
    )
    def test_sql_kind_property(
        self,
        sql_kind: str,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that sql_kind property returns correct value."""
        statement_json = statement_response_factory(sql_kind=sql_kind)
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.sql_kind == sql_kind

    def test_sql_kind_raises_when_no_traits(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that sql_kind property raises InterfaceError when
        failed statement traits are missing."""

        statement_json = statement_response_factory(phase="FAILED")
        statement = Statement.from_response(mock_connection, statement_json)

        with pytest.raises(
            InterfaceError,
            match="Statement traits are not available.",
        ):
            _ = statement.sql_kind

    @pytest.mark.parametrize(
        "sql_kind,expected",
        [
            ("CREATE_TABLE", True),
            ("DROP_TABLE", True),
            ("CREATE_VIEW", True),
            ("DROP_VIEW", True),
            ("ALTER_TABLE", True),
            ("SELECT", False),
            ("INSERT", False),
            ("UPDATE", False),
            ("DELETE", False),
        ],
    )
    def test_is_pure_ddl(
        self,
        sql_kind: str,
        expected: bool,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_pure_ddl property correctly identifies pure DDL statements."""
        statement_json = statement_response_factory(sql_kind=sql_kind)
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.is_pure_ddl == expected

    def test_is_pure_ddl_raises_when_no_traits(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_pure_ddl property raises InterfaceError when
        failed statement traits are missing."""

        statement_json = statement_response_factory(phase="FAILED")
        statement = Statement.from_response(mock_connection, statement_json)

        with pytest.raises(
            InterfaceError,
            match="Statement traits are not available.",
        ):
            _ = statement.is_pure_ddl

    def test_scaling_status_present(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that scaling_status property returns the scaling status when present."""
        statement_json = statement_response_factory()
        # The default factory includes a scaling_status
        statement = Statement.from_response(mock_connection, statement_json)

        scaling_status = statement.scaling_status
        assert isinstance(scaling_status, dict)
        assert "scaling_state" in scaling_status
        assert scaling_status["scaling_state"] == "OK"
        assert "last_updated" in scaling_status

    def test_scaling_status_absent(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that scaling_status property returns empty dict when absent."""
        statement_json = statement_response_factory()
        # Remove scaling_status from the response
        del statement_json["status"]["scaling_status"]
        statement = Statement.from_response(mock_connection, statement_json)

        scaling_status = statement.scaling_status
        assert scaling_status == {}

    def test_scaling_status_with_pool_exhausted(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that scaling_status property correctly returns POOL_EXHAUSTED state."""
        statement_json = statement_response_factory(phase="PENDING")
        # Modify the scaling_status to indicate pool exhaustion
        statement_json["status"]["scaling_status"]["scaling_state"] = "POOL_EXHAUSTED"
        statement = Statement.from_response(mock_connection, statement_json)

        scaling_status = statement.scaling_status
        assert scaling_status["scaling_state"] == "POOL_EXHAUSTED"

    @pytest.mark.parametrize(
        "phase,scaling_state,expected",
        [
            ("PENDING", "POOL_EXHAUSTED", True),
            ("PENDING", "OK", False),
            ("PENDING", "SCALING", False),
            ("RUNNING", "OK", False),
        ],
    )
    def test_is_pool_exhausted(
        self,
        phase: str,
        scaling_state: str,
        expected: bool,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_pool_exhausted property correctly identifies pool exhaustion state."""
        statement_json = statement_response_factory(phase=phase)
        statement_json["status"]["scaling_status"]["scaling_state"] = scaling_state
        statement = Statement.from_response(mock_connection, statement_json)

        assert statement.is_pool_exhausted == expected

    def test_is_pool_exhausted_no_scaling_status(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_pool_exhausted returns False when scaling_status is absent."""
        statement_json = statement_response_factory(phase="PENDING")
        # Remove scaling_status entirely
        del statement_json["status"]["scaling_status"]
        statement = Statement.from_response(mock_connection, statement_json)

        # Should return False when no scaling_status is present
        assert statement.is_pool_exhausted is False

    def test_is_pool_exhausted_no_scaling_state(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
    ):
        """Test that is_pool_exhausted returns False when scaling_state is absent."""
        statement_json = statement_response_factory(phase="PENDING")
        # Remove just the scaling_state key
        del statement_json["status"]["scaling_status"]["scaling_state"]
        statement = Statement.from_response(mock_connection, statement_json)

        # Should return False when scaling_state is not present
        assert statement.is_pool_exhausted is False


@pytest.mark.unit
class TestStatementFromResponse:
    """Tests for Statement.from_response class method error paths."""

    def test_hates_unknown_status_phase(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that from_response raises on unknown status.phase."""
        with pytest.raises(OperationalError, match="Received an unknown phase for statement"):
            Statement.from_response(mock_connection, statement_response_factory(phase="UNKNOWN"))

    def test_hates_missing_keys(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that from_response raises if required keys are missing."""
        incomplete_json = statement_response_factory()
        del incomplete_json["spec"]

        with pytest.raises(
            OperationalError, match="Error parsing statement response, missing 'spec'"
        ):
            Statement.from_response(mock_connection, incomplete_json)

    def test_parses_row_result_schema(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
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
        statement = Statement.from_response(mock_connection, statement_json)
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
    def schema_factory(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ) -> SchemaFactory:
        def _schema_maker(
            schema_columns: list[dict[str, Any]],
        ) -> Schema:
            statement_dict = statement_response_factory(
                schema_columns=schema_columns,
            )
            statement = Statement.from_response(mock_connection, statement_dict)
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


@pytest.mark.unit
class TestStatementEndUserLabels:
    """Tests for Statement.end_user_labels property."""

    def test_labels_present(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that end_user_labels property returns correct prefix-stripped labels
        when present."""
        statement_json = statement_response_factory(
            labels={"user.confluent.io/foo": "true", "user.confluent.io/bar": "true"},
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.end_user_labels == ["foo", "bar"]

    def test_empty_labels(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that end_user_labels property returns empty list when labels are empty."""
        statement_json = statement_response_factory()
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.end_user_labels == []

    def test_missing_metadata_labels_dict_raises(
        self, mock_connection: Connection, statement_response_factory: StatementResponseFactory
    ):
        """Test that end_user_labels property raises when metadata has no labels member."""
        statement_json = statement_response_factory()
        # Remove end_user_labels from the response
        del statement_json["metadata"]["labels"]
        statement = Statement.from_response(mock_connection, statement_json)
        with pytest.raises(InterfaceError):
            _ = statement.end_user_labels


@pytest.mark.unit
class TestStatementCanFetchResults:
    """Comprehensive tests for Statement.can_fetch_results() method.

    Tests all combinations of:
    - Execution modes: SNAPSHOT, SNAPSHOT_DDL, STREAMING_QUERY, STREAMING_DDL
    - Statement types: Pure DDL, Bounded append-only, Bounded non-append-only, Unbounded
    - Phases: PENDING, RUNNING, COMPLETED, FAILED, STOPPED
    """

    @pytest.mark.parametrize("phase", ["PENDING", "RUNNING"])
    def test_snapshot_mode_bounded_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In snapshot mode, bounded statements are not ready until COMPLETED."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.SNAPSHOT)

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "FAILED"])
    def test_snapshot_mode_bounded_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In snapshot mode, bounded statements are ready in terminal states."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.SNAPSHOT)

    @pytest.mark.parametrize("phase", ["PENDING", "RUNNING"])
    def test_streaming_query_bounded_non_append_only_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, bounded non-append-only queries must wait for COMPLETED."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
            sql_kind="SELECT",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_query_bounded_non_append_only_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, bounded non-append-only queries are ready in terminal states."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
            sql_kind="SELECT",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["PENDING"])
    def test_streaming_query_bounded_append_only_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, bounded append-only queries are not ready until RUNNING."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["RUNNING", "COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_query_bounded_append_only_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, bounded append-only queries are ready when RUNNING or terminal."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["PENDING"])
    def test_streaming_query_unbounded_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, unbounded queries are not ready until RUNNING."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=False,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["RUNNING", "COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_query_unbounded_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming mode, unbounded queries are ready when RUNNING or terminal."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=False,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_QUERY)

    @pytest.mark.parametrize("phase", ["PENDING", "RUNNING"])
    def test_streaming_ddl_pure_ddl_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, pure DDL must wait for terminal state."""
        statement_json = statement_response_factory(
            phase=phase,
            sql_kind="CREATE_TABLE",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_DDL)

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_ddl_pure_ddl_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, pure DDL is ready in terminal states."""
        statement_json = statement_response_factory(
            phase=phase,
            sql_kind="CREATE_TABLE",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_DDL)

    @pytest.mark.parametrize("phase", ["PENDING", "RUNNING"])
    def test_streaming_ddl_bounded_non_append_only_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, bounded non-append-only queries must wait for COMPLETED."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
            sql_kind="SELECT",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_DDL)

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_ddl_bounded_non_append_only_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, bounded non-append-only queries are ready in terminal states."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=True,
            is_append_only=False,
            sql_kind="SELECT",
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_DDL)

    @pytest.mark.parametrize("phase", ["PENDING"])
    def test_streaming_ddl_unbounded_not_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, unbounded queries are not ready until RUNNING."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=False,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert not statement.can_fetch_results(ExecutionMode.STREAMING_DDL)

    @pytest.mark.parametrize("phase", ["RUNNING", "COMPLETED", "STOPPED", "FAILED"])
    def test_streaming_ddl_unbounded_ready(
        self,
        mock_connection: Connection,
        statement_response_factory: StatementResponseFactory,
        phase: str,
    ):
        """In streaming DDL mode, unbounded queries are ready when RUNNING or terminal."""
        statement_json = statement_response_factory(
            phase=phase,
            is_bounded=False,
            is_append_only=True,
        )
        statement = Statement.from_response(mock_connection, statement_json)
        assert statement.can_fetch_results(ExecutionMode.STREAMING_DDL)
