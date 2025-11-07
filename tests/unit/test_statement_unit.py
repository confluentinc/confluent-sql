import pytest

from confluent_sql import DatabaseError, OperationalError
from confluent_sql.statement import Phase, Statement

"""Unit tests over Statement class."""


class TestStatementIsReady:
    """Tests for Statement.is_ready property."""

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED"])
    def test_bounded_is_ready(self, statement_json_factory, phase):
        """Test that a bounded statement in COMPLETED or
        STOPPED phase is ready."""
        statement_json = statement_json_factory(
            phase=phase,
            is_bounded=True,
        )
        statement = Statement.from_response(statement_json)
        assert statement.is_ready

    def test_bounded_not_ready(self, statement_json_factory):
        """Test that a bounded statement not in COMPLETED or
        STOPPED phase is not ready."""
        statement_json = statement_json_factory(
            phase="RUNNING",
            is_bounded=True,
        )
        statement = Statement.from_response(statement_json)
        assert not statement.is_ready

    @pytest.mark.parametrize("phase", ["COMPLETED", "STOPPED", "RUNNING"])
    def test_unbounded_is_ready(self, statement_json_factory, phase):
        """Test that an unbounded statement in COMPLETED, STOPPED,
        or RUNNING phase is ready."""
        statement_json = statement_json_factory(
            phase=phase,
            is_bounded=False,
        )
        statement = Statement.from_response(statement_json)
        assert statement.is_ready

    def test_unbounded_pending_not_ready(self, statement_json_factory):
        """Test that an unbounded statement not in PENDING phase is not ready."""
        statement_json = statement_json_factory(
            phase="PENDING",
            is_bounded=False,
        )
        statement = Statement.from_response(statement_json)
        assert not statement.is_ready


class TestStatementProperties:
    """Tests for various Statement properties."""

    def test_compute_pool_id(self, statement_json_factory):
        """Test that compute_pool_id property returns correct value."""
        statement_json = statement_json_factory(compute_pool_id="test-pool-id")
        statement = Statement.from_response(statement_json)
        assert statement.compute_pool_id == "test-pool-id"

    def test_principal(self, statement_json_factory):
        """Test that principal property returns correct value."""
        statement_json = statement_json_factory(principal="test-principal")
        statement = Statement.from_response(statement_json)
        assert statement.principal == "test-principal"

    def test_phase_property(self, statement_json_factory):
        """Test that phase property returns correct Phase enum."""
        statement_json = statement_json_factory(phase="RUNNING")
        statement = Statement.from_response(statement_json)
        assert statement.phase == Phase.RUNNING

    def test_phase_when_deleted(self, statement_json_factory):
        """Test that phase property returns DELETED when statement is deleted."""
        statement_json = statement_json_factory()
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
    def test_is_running(self, statement_json_factory, phase, expected):
        """Test that is_running property returns correct boolean."""
        statement_json = statement_json_factory(phase=phase)
        statement = Statement.from_response(statement_json)
        assert statement.is_running == expected


class TestStatementFromResponse:
    """Tests for Statement.from_response class method error paths."""

    def test_hates_unknown_status_phase(self, statement_json_factory):
        """Test that from_response raises on unknown status.phase."""
        with pytest.raises(
            OperationalError, match="Received an unknown phase for statement"
        ):
            Statement.from_response(statement_json_factory(phase="UNKNOWN"))

    def test_raises_databaseerror_if_failed(self, statement_json_factory):
        """Test that a failed statement raises DatabaseError with details when if the statement failed."""
        failed_statement_json = statement_json_factory(
            phase="FAILED", status_detail="Some error"
        )
        with pytest.raises(DatabaseError, match="Some error"):
            Statement.from_response(failed_statement_json)

    def test_hates_missing_keys(self, statement_json_factory):
        """Test that from_response raises if required keys are missing."""
        incomplete_json = statement_json_factory()
        del incomplete_json["spec"]

        with pytest.raises(
            OperationalError, match="Error parsing statement response, missing 'spec'"
        ):
            Statement.from_response(incomplete_json)
