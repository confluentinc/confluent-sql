"""Unit tests for changelog_compressor module and get_current_snapshot() API."""

from unittest.mock import MagicMock

import pytest

from confluent_sql.changelog_compressor import (
    NoUpsertColumnsCompressor,
    UpsertColumnsCompressor,
    create_changelog_compressor,
)
from confluent_sql.cursor import Cursor
from confluent_sql.exceptions import InterfaceError, StatementStoppedError
from confluent_sql.result_readers import ChangeloggedRow
from confluent_sql.statement import Op, Schema, Statement, Traits


@pytest.fixture
def mock_cursor():
    """Create a mock cursor for testing compressors."""
    cursor = MagicMock(spec=Cursor)
    cursor.arraysize = 100
    cursor.as_dict = False  # Use the public property
    cursor.returns_changelog = True
    cursor.may_have_results = True  # Allow generator to yield snapshots

    # Mock statement with schema and traits
    statement = MagicMock(spec=Statement)
    statement.name = "test-statement"
    statement.phase = None
    statement.traits = MagicMock(spec=Traits)
    statement.traits.upsert_columns = None

    # Create a simple schema
    schema = MagicMock(spec=Schema)
    col_id = MagicMock()
    col_id.name = "id"
    col_value = MagicMock()
    col_value.name = "value"
    col_count = MagicMock()
    col_count.name = "count"
    schema.columns = [col_id, col_value, col_count]
    statement.schema = schema

    cursor._statement = statement
    cursor.statement = statement  # Add public statement property

    return cursor


@pytest.mark.unit
class TestUpsertColumnsCompressor:
    """Tests for UpsertColumnsCompressor with both tuple and dict formats."""

    def test_insert_operations_tuple(self, mock_cursor):
        """Test handling of INSERT operations with tuple format."""
        mock_cursor._statement.traits.upsert_columns = [0]  # id is the key
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return INSERT operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
            ],
            [],  # End of results
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)

    def test_update_operations(self, mock_cursor):
        """Test handling of UPDATE_BEFORE and UPDATE_AFTER operations."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with updates
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),  # Update count
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 15)

    def test_delete_operations(self, mock_cursor):
        """Test handling of DELETE operations."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with delete
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (2, "b", 20)

    def test_compound_key(self, mock_cursor):
        """Test with compound upsert columns."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # id and value are keys
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with compound key operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "b", 20)),  # Same id, different value
                ChangeloggedRow(Op.INSERT, (2, "a", 30)),  # Different id, same value
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 3
        # Order preserved
        assert (1, "a", 10) in snapshot
        assert (1, "b", 20) in snapshot
        assert (2, "a", 30) in snapshot

    def test_deep_copy_returned(self, mock_cursor):
        """Test that snapshots() returns a deep copy across calls."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, ["nested", "list"], 10))],
            [],
        ]

        snapshot1 = next(compressor.snapshots())

        # Reset fetchmany for second call
        mock_cursor.fetchmany.side_effect = [
            [],
        ]  # No new results

        snapshot2 = next(compressor.snapshots())

        # Modify the first snapshot
        # Cast to list since we know it's a list from the test setup
        row1 = snapshot1[0]
        assert isinstance(row1, tuple)
        nested_list = row1[1]
        if isinstance(nested_list, list):
            nested_list.append("modified")

        # Second snapshot should not be affected
        row2 = snapshot2[0]
        assert isinstance(row2, tuple)
        assert row2[1] == ["nested", "list"]

    def test_dict_operations(self, mock_cursor):
        """Test compressor with dict results."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with dict results
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b", "count": 15}),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b", "count": 15}

    def test_key_extraction_from_dict(self, mock_cursor):
        """Test key extraction from dictionary rows."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [1, 2]  # value and count are keys
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.INSERT, {"id": 2, "value": "a", "count": 10}),  # Same key
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Should only have one row since keys are the same
        assert len(snapshot) == 1
        row = snapshot[0]
        assert isinstance(row, dict)
        assert row["id"] == 2  # Last one wins

    def test_key_extraction_from_dict_uses_schema(self, mock_cursor):
        """Test that key extraction from dict correctly uses schema columns."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]  # id is the key
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with dict operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b", "count": 20}),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Verify key extraction worked correctly (used schema to map dict to indices)
        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b", "count": 20}


@pytest.mark.unit
class TestNoUpsertColumnsCompressor:
    """Tests for NoUpsertColumnsCompressor."""

    def test_without_keys(self, mock_cursor):
        """Test compressor without upsert columns."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate allowed
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 2  # Both rows kept
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (1, "a", 10)

    def test_scan_based_updates(self, mock_cursor):
        """Test scan-based UPDATE operations."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 15)  # Updated
        assert snapshot[1] == (2, "b", 20)  # Unchanged

    def test_scan_based_delete(self, mock_cursor):
        """Test scan-based DELETE operations."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Deletes the last one
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1  # One remains
        assert snapshot[0] == (1, "a", 10)

    def test_position_adjustment_after_delete(self, mock_cursor):
        """Test that UPDATE_BEFORE must be immediately followed by UPDATE_AFTER."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE between UPDATE_BEFORE and UPDATE_AFTER
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.INSERT, (3, "c", 30)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (3, "c", 30)),
                # ERROR: Can't have DELETE while UPDATE_BEFORE is pending
                ChangeloggedRow(Op.DELETE, (2, "b", 20)),
            ],
            [],
        ]

        # Should raise error because UPDATE_BEFORE must be followed by UPDATE_AFTER
        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())

    def test_dict_without_keys(self, mock_cursor):
        """Test dict compressor without upsert columns."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a"}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a"}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b"}),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b"}


@pytest.mark.unit
class TestCompressorValidation:
    """Test validation in ChangelogCompressor and its subclasses."""

    def test_all_compressors_require_changelog_cursor(self, mock_cursor):
        """Test compressor types raise InterfaceError when cursor.returns_changelog is False."""
        # Set cursor to not return changelog
        mock_cursor.returns_changelog = False

        # Test UpsertColumnsCompressor
        mock_cursor._statement.traits.upsert_columns = [0]
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Test NoUpsertColumnsCompressor
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_upsert_columns_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without upsert columns."""
        # Remove upsert columns from statement
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(
            InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"
        ):
            UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_traits_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without traits."""
        # Remove traits entirely
        mock_cursor._statement.traits = None

        with pytest.raises(
            InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"
        ):
            UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

    def test_all_compressors_require_schema(self, mock_cursor):
        """Test that all compressor types raise InterfaceError when statement has no schema."""
        # Remove schema from statement
        mock_cursor._statement.schema = None

        # Test with upsert columns (though it won't get that far)
        mock_cursor._statement.traits.upsert_columns = [0]

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Test without upsert columns
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

    def test_validation_order_changelog_before_schema(self, mock_cursor):
        """Test that changelog validation happens before schema validation."""
        # Set cursor to not return changelog AND remove schema
        mock_cursor.returns_changelog = False
        mock_cursor._statement.schema = None

        # Should get the changelog error first, not the schema error
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)


@pytest.mark.unit
class TestFactoryFunction:
    """Tests for the create_changelog_compressor factory function."""

    def test_factory_selects_correct_compressor(self, mock_cursor):
        """Test that the factory function selects the correct compressor class."""
        # Test with upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = [0]

        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, UpsertColumnsCompressor)

        # Test with upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, UpsertColumnsCompressor)

        # Test without upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = None
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, NoUpsertColumnsCompressor)

        # Test without upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, NoUpsertColumnsCompressor)

    def test_factory_validates_changelog_cursor(self, mock_cursor):
        """Test that the factory function validates cursor returns changelog."""
        mock_cursor.returns_changelog = False

        with pytest.raises(
            InterfaceError, match="can only be created for streaming non-append-only"
        ):
            create_changelog_compressor(mock_cursor, mock_cursor._statement)


@pytest.mark.unit
class TestChangelogCompressorCreation:
    """Tests for creating compressors from cursor."""

    def test_create_compressor_for_non_changelog(self, mock_cursor):
        """Test that creating compressor for non-changelog query raises error."""
        mock_cursor.returns_changelog = False

        # Bind the real method to the mock
        mock_cursor.changelog_compressor = Cursor.changelog_compressor.__get__(mock_cursor, Cursor)

        with pytest.raises(
            InterfaceError, match="can only be created for streaming non-append-only"
        ):
            mock_cursor.changelog_compressor()

    def test_create_compressor_without_statement(self, mock_cursor):
        """Test that creating compressor without a statement raises error."""
        mock_cursor._statement = None

        # Bind the real method to the mock
        mock_cursor.changelog_compressor = Cursor.changelog_compressor.__get__(mock_cursor, Cursor)

        with pytest.raises(
            InterfaceError, match="Cannot create changelog compressor without a statement"
        ):
            mock_cursor.changelog_compressor()


@pytest.mark.unit
class TestBatchSize:
    """Tests for batch size handling."""

    def test_custom_batch_size(self, mock_cursor):
        """Test using custom batch size for fetching."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        # Use custom batch size
        snapshot = next(compressor.snapshots(fetch_batchsize=50))

        # Verify fetchmany was called with custom size
        mock_cursor.fetchmany.assert_called_with(50)
        assert len(snapshot) == 1

    def test_default_batch_size(self, mock_cursor):
        """Test using cursor's arraysize as default."""
        mock_cursor.arraysize = 200
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Verify fetchmany was called with cursor's arraysize
        mock_cursor.fetchmany.assert_called_with(200)
        assert len(snapshot) == 1


@pytest.mark.unit
class TestCloseMethod:
    """Tests for the close() method."""

    def test_close_calls_cursor_close(self, mock_cursor):
        """Test that close() calls the cursor's close method."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]
        next(compressor.snapshots())

        # Close the compressor
        compressor.close()

        # Verify cursor.close() was called
        mock_cursor.close.assert_called_once()

    def test_close_clears_upsert_compressor_state(self, mock_cursor):
        """Test that close() clears internal state for UpsertColumnsCompressor."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
            ],
            [],
        ]
        next(compressor.snapshots())

        # Verify data exists
        assert len(compressor._rows_by_key) == 2

        # Close the compressor
        compressor.close()

        # Verify internal state is cleared
        assert len(compressor._rows_by_key) == 0

    def test_close_clears_no_upsert_compressor_state(self, mock_cursor):
        """Test that close() clears internal state for NoUpsertColumnsCompressor."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
            ],
            [],
        ]
        next(compressor.snapshots())

        # Verify data exists
        assert len(compressor._rows) == 2
        assert compressor._pending_update_position == 0

        # Close the compressor
        compressor.close()

        # Verify internal state is cleared
        assert len(compressor._rows) == 0
        assert compressor._pending_update_position is None

    def test_close_with_dict_compressor(self, mock_cursor):
        """Test that close() works with dict compressor."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10})],
            [],
        ]
        next(compressor.snapshots())

        # Close the compressor
        compressor.close()

        # Verify cursor.close() was called and state is cleared
        mock_cursor.close.assert_called_once()
        assert len(compressor._rows_by_key) == 0

    def test_close_idempotent(self, mock_cursor):
        """Test that close() can be called multiple times safely."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Close multiple times
        compressor.close()
        compressor.close()
        compressor.close()

        # Cursor close should be called each time (3 times total)
        assert mock_cursor.close.call_count == 3


@pytest.mark.unit
class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_missing_update_after(self, mock_cursor):
        """Test handling UPDATE_BEFORE without matching UPDATE_AFTER."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with missing UPDATE_AFTER
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                # UPDATE_AFTER is missing
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Original row should remain unchanged
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

    def test_update_after_without_before(self, mock_cursor):
        """Test bare UPDATE_AFTER requires UPDATE_BEFORE in NoUpsertColumnsCompressor."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with bare UPDATE_AFTER
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        # Should raise error because UPDATE_BEFORE is required
        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER without a preceding UPDATE_BEFORE",
        ):
            next(compressor.snapshots())

    def test_multiple_batches(self, mock_cursor):
        """Test fetching across multiple batches."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return multiple batches
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],  # First batch
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],  # Second batch
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Third batch
            [],  # End of results
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 3
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)
        assert snapshot[2] == (3, "c", 30)

    def test_empty_results(self, mock_cursor):
        """Test with no results."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return empty
        mock_cursor.fetchmany.side_effect = [[]]

        snapshot = next(compressor.snapshots())

        assert snapshot == []

    def test_overwriting_pending_update(self, mock_cursor):
        """Test that UPDATE_BEFORE must be followed by UPDATE_AFTER immediately."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with two UPDATE_BEFOREs in a row (invalid)
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                # ERROR: Can't have another UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
            ],
            [],
        ]

        # Should raise error because UPDATE_BEFORE must be followed by UPDATE_AFTER
        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())


@pytest.mark.unit
class TestUpsertColumnsCompressorErrorCases:
    """Tests for error cases in UpsertColumnsCompressor when keys are not found."""

    def test_update_before_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_BEFORE for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (999, "z", 99)),  # Key doesn't exist
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(999,\)",
        ):
            next(compressor.snapshots())

    def test_update_after_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (999, "z", 99)),  # Key doesn't exist
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: \(999,\)",
        ):
            next(compressor.snapshots())

    def test_update_before_after_delete_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE after DELETE raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE followed by UPDATE_BEFORE for the same key
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),  # Key no longer exists
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: \(1,\)",
        ):
            next(compressor.snapshots())

    def test_update_after_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER without any prior INSERT raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER as the first operation
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 10)),  # No prior INSERT
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: \(1,\)",
        ):
            next(compressor.snapshots())

    def test_update_before_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_BEFORE for a compound key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "z", 99)),  # Different second key component
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(1, 'z'\)",
        ):
            next(compressor.snapshots())

    def test_update_after_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER for a compound key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (2, "a", 20)),  # Different first key component
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: "
            r"\(2, 'a'\)",
        ):
            next(compressor.snapshots())

    def test_update_before_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_BEFORE for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 999, "value": "z", "count": 99}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(999,\)",
        ):
            next(compressor.snapshots())

    def test_update_after_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 999, "value": "z", "count": 99}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: \(999,\)",
        ):
            next(compressor.snapshots())

    def test_delete_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that DELETE for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (999, "z", 99)),  # Key doesn't exist
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a key that does not exist in current state: \(999,\)",
        ):
            next(compressor.snapshots())

    def test_delete_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that DELETE without any prior INSERT raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE as the first operation
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # No prior INSERT
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a key that does not exist in current state: \(1,\)",
        ):
            next(compressor.snapshots())

    def test_delete_twice_same_key_raises_interface_error(self, mock_cursor):
        """Test that deleting the same key twice raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with two DELETEs for the same key
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # First delete is OK
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Second delete should fail
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a key that does not exist in current state: \(1,\)",
        ):
            next(compressor.snapshots())

    def test_delete_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that DELETE with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE for a compound key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "z", 99)),  # Different second key component
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a key that does not exist in current state: \(1, 'z'\)",
        ):
            next(compressor.snapshots())

    def test_delete_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that DELETE for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE for a key that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.DELETE, {"id": 999, "value": "z", "count": 99}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a key that does not exist in current state: \(999,\)",
        ):
            next(compressor.snapshots())

    def test_successful_delete_does_not_raise_error(self, mock_cursor):
        """Test that DELETE for an existing key works correctly."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with successful DELETE
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Should succeed
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Verify delete worked
        assert len(snapshot) == 1
        assert snapshot[0] == (2, "b", 20)


@pytest.mark.unit
class TestNoUpsertColumnsCompressorErrorCases:
    """Tests for error cases in NoUpsertColumnsCompressor when rows are not found."""

    def test_update_before_with_nonexistent_row_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for a non-existent row raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_BEFORE for a row that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (999, "z", 99)),  # Row doesn't exist
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_update_after_without_update_before_raises_interface_error(self, mock_cursor):
        """Test that bare UPDATE_AFTER without UPDATE_BEFORE raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER without UPDATE_BEFORE
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER without a preceding UPDATE_BEFORE",
        ):
            next(compressor.snapshots())

    def test_update_before_after_delete_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE after DELETE raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE followed by UPDATE_BEFORE for the same row
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),  # Row no longer exists
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_delete_with_nonexistent_row_raises_interface_error(self, mock_cursor):
        """Test that DELETE for a non-existent row raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE for a row that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (999, "z", 99)),  # Row doesn't exist
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_delete_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that DELETE without any prior INSERT raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE as the first operation
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # No prior INSERT
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_delete_twice_same_row_raises_interface_error(self, mock_cursor):
        """Test that deleting the same row twice raises InterfaceError."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with two DELETEs for the same row
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # First delete is OK
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Second delete should fail
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_update_before_finds_most_recent_duplicate(self, mock_cursor):
        """Test that UPDATE_BEFORE finds the most recent duplicate row."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with duplicate rows
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),  # Updates most recent
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Should have two rows: one unchanged, one updated
        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)  # First one unchanged
        assert snapshot[1] == (1, "a", 20)  # Second one updated

    def test_delete_removes_most_recent_duplicate(self, mock_cursor):
        """Test that DELETE removes the most recent duplicate row."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with duplicate rows
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Deletes most recent
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Should have one row remaining
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

    def test_update_before_with_dict_rows_nonexistent_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for dict rows with non-existent row raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_BEFORE for a row that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 999, "value": "z", "count": 99}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_BEFORE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_update_after_without_before_dict_rows_raises_interface_error(self, mock_cursor):
        """Test that bare UPDATE_AFTER for dict rows raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with UPDATE_AFTER without UPDATE_BEFORE
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b", "count": 20}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER without a preceding UPDATE_BEFORE",
        ):
            next(compressor.snapshots())

    def test_delete_with_dict_rows_nonexistent_raises_interface_error(self, mock_cursor):
        """Test that DELETE for dict rows with non-existent row raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with DELETE for a row that doesn't exist
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.DELETE, {"id": 999, "value": "z", "count": 99}),
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE for a row that does not exist in current state",
        ):
            next(compressor.snapshots())

    def test_successful_operations_do_not_raise_errors(self, mock_cursor):
        """Test that valid operation sequences work correctly."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with valid operation sequence
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),
                ChangeloggedRow(Op.DELETE, (2, "b", 20)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        # Verify operations worked correctly
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 15)


@pytest.mark.unit
class TestUpsertColumnsCompressorUpdateSequencing:
    """Tests for UPDATE_BEFORE/UPDATE_AFTER sequencing in UpsertColumnsCompressor."""

    def test_bare_update_after_with_existing_key_succeeds(self, mock_cursor):
        """Test that bare UPDATE_AFTER works when key exists."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)

    def test_insert_after_update_before_raises_error(self, mock_cursor):
        """Test that INSERT after UPDATE_BEFORE raises error."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),  # ERROR
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received INSERT while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())

    def test_delete_after_update_before_raises_error(self, mock_cursor):
        """Test that DELETE after UPDATE_BEFORE raises error."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # ERROR
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())

    def test_paired_update_before_after_succeeds(self, mock_cursor):
        """Test that UPDATE_BEFORE immediately followed by UPDATE_AFTER works."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)


@pytest.mark.unit
class TestNoUpsertColumnsCompressorUpdateSequencing:
    """Tests for UPDATE_BEFORE/UPDATE_AFTER sequencing in NoUpsertColumnsCompressor."""

    def test_bare_update_after_requires_update_before(self, mock_cursor):
        """Test that bare UPDATE_AFTER requires UPDATE_BEFORE."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE (should fail)
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        # Should raise error because UPDATE_BEFORE is required
        with pytest.raises(
            InterfaceError,
            match=r"Received UPDATE_AFTER without a preceding UPDATE_BEFORE",
        ):
            next(compressor.snapshots())

    def test_insert_after_update_before_raises_error(self, mock_cursor):
        """Test that INSERT after UPDATE_BEFORE raises error."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),  # ERROR
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received INSERT while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())

    def test_delete_after_update_before_raises_error(self, mock_cursor):
        """Test that DELETE after UPDATE_BEFORE raises error."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # ERROR
            ],
            [],
        ]

        with pytest.raises(
            InterfaceError,
            match=r"Received DELETE while an UPDATE_BEFORE is pending",
        ):
            next(compressor.snapshots())

    def test_paired_update_before_after_succeeds(self, mock_cursor):
        """Test that UPDATE_BEFORE immediately followed by UPDATE_AFTER works."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)


@pytest.mark.unit
class TestSnapshotsGeneratorTermination:
    """Tests for snapshots() generator termination behavior."""

    def test_generator_terminates_when_may_have_results_becomes_false(self, mock_cursor):
        """Test snapshots() raises StatementStoppedError when may_have_results is False."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],  # End of first snapshot
            [],  # Additional calls to fetchmany for next iteration (safety)
        ]

        # First snapshot - may_have_results is True (set in mock_cursor fixture)
        gen = compressor.snapshots()
        snapshot = next(gen)
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

        # Change may_have_results to False for next iteration
        mock_cursor.may_have_results = False

        # Next iteration should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        # Verify exception attributes
        assert exc_info.value.statement_name == "test-statement"
        assert exc_info.value.statement is mock_cursor.statement

    def test_generator_yields_multiple_snapshots(self, mock_cursor):
        """Test snapshots() yields multiple snapshots then raises StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return data then empty for each snapshot
        # Pattern: data, [], data, [], data, []
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],  # Snapshot 1 data
            [],  # End snapshot 1
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],  # Snapshot 2 data
            [],  # End snapshot 2
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Snapshot 3 data
            [],  # End snapshot 3
            [],  # Safety for next iteration
        ]

        gen = compressor.snapshots()

        snapshot1 = next(gen)
        assert len(snapshot1) == 1
        assert snapshot1[0] == (1, "a", 10)

        snapshot2 = next(gen)
        assert len(snapshot2) == 2
        assert (1, "a", 10) in snapshot2
        assert (2, "b", 20) in snapshot2

        snapshot3 = next(gen)
        assert len(snapshot3) == 3
        assert (1, "a", 10) in snapshot3
        assert (2, "b", 20) in snapshot3
        assert (3, "c", 30) in snapshot3

        # Change may_have_results to False
        mock_cursor.may_have_results = False

        # Next call should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_no_events(self, mock_cursor):
        """Test generator raises StatementStoppedError when may_have_results=False."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock may_have_results to be False immediately
        mock_cursor.may_have_results = False

        # Generator should raise StatementStoppedError immediately
        with pytest.raises(StatementStoppedError) as exc_info:
            next(compressor.snapshots())

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_empty_snapshots(self, mock_cursor):
        """Test generator yields empty snapshots then raises StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return empty batches (3 times, then raise on next)
        mock_cursor.fetchmany.side_effect = [
            [],  # Snapshot 1 (empty)
            [],  # Snapshot 2 (empty)
            [],  # Safety for next iteration check
        ]

        # Consume the generator
        gen = compressor.snapshots()
        snapshot1 = next(gen)
        snapshot2 = next(gen)

        # Should yield two empty snapshots
        assert snapshot1 == []
        assert snapshot2 == []

        # Change may_have_results to False
        mock_cursor.may_have_results = False

        # Next should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_trailing_events_before_termination(self, mock_cursor):
        """Test generator processes all events before raising StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return data across two snapshots
        mock_cursor.fetchmany.side_effect = [
            # Snapshot 1
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
            ],
            [],  # End of first snapshot
            # Snapshot 2
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Trailing events
            [],  # End of second snapshot
            [],  # Safety for next iteration
        ]

        # Consume the generator
        gen = compressor.snapshots()
        snapshot1 = next(gen)
        snapshot2 = next(gen)

        # Should have two snapshots with accumulated state
        assert len(snapshot1) == 2  # First snapshot has 2 rows
        assert len(snapshot2) == 3  # Second snapshot has 3 rows (accumulated)

        # Change may_have_results to False
        mock_cursor.may_have_results = False

        # Next should raise StatementStoppedError
        with pytest.raises(StatementStoppedError):
            next(gen)

    def test_generator_terminates_cleanly_on_for_loop(self, mock_cursor):
        """Test that for loop over snapshots() supports client-controlled early termination."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "x", 10))],  # Snapshot 1
            [],  # End of first snapshot
            [ChangeloggedRow(Op.INSERT, (2, "y", 20))],  # Snapshot 2
            [],  # End of second snapshot
            [],  # Safety for next iteration
        ]

        # Use for loop pattern - break after 2 snapshots (client can stop whenever it wants)
        snapshots_seen = []
        gen = compressor.snapshots()
        for i, snapshot in enumerate(gen):
            snapshots_seen.append(snapshot)
            if i >= 1:  # After 2 snapshots, client breaks
                break

        # Should have iterated exactly twice before break
        assert len(snapshots_seen) == 2


@pytest.mark.unit
class TestGetCurrentSnapshot:
    """Tests for the get_current_snapshot() method."""

    def test_single_batch_fetch_with_upsert_columns(self, mock_cursor):
        """Test get_current_snapshot with single batch (UpsertColumnsCompressor)."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock single batch
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        snapshot = compressor.get_current_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)
        mock_cursor.fetchmany.assert_called()

    def test_multiple_batch_fetch_with_upsert_columns(self, mock_cursor):
        """Test get_current_snapshot loops until empty batch (UpsertColumnsCompressor)."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock multiple batches - should loop until empty
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],  # Batch 1
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],  # Batch 2
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Batch 3
            [],  # Empty - stops loop
        ]

        snapshot = compressor.get_current_snapshot()

        assert len(snapshot) == 3
        assert mock_cursor.fetchmany.call_count == 4  # 3 batches + 1 empty

    def test_empty_batch_no_events(self, mock_cursor):
        """Test get_current_snapshot with no events available."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock empty batch immediately
        mock_cursor.fetchmany.side_effect = [[]]

        snapshot = compressor.get_current_snapshot()

        assert snapshot == []
        assert mock_cursor.fetchmany.call_count == 1

    def test_deep_copy_verification(self, mock_cursor):
        """Test that returned snapshot is a deep copy (including nested mutable structures)."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Insert row with mutable dict structure
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "metadata": {"count": 10}})],
            [],
        ]

        snapshot1 = compressor.get_current_snapshot()
        assert isinstance(snapshot1[0], dict)

        # Mutate nested structure in returned snapshot
        row1 = snapshot1[0]
        metadata = row1["metadata"]
        assert isinstance(metadata, dict)
        metadata["count"] = 999

        # Get another snapshot - should be unaffected by mutation
        mock_cursor.fetchmany.side_effect = [[]]
        snapshot2 = compressor.get_current_snapshot()

        # Verify snapshot2 is unaffected by mutations to snapshot1
        row2 = snapshot2[0]
        assert isinstance(row2, dict)
        assert row2["metadata"]["count"] == 10  # Original unchanged
        assert row1["metadata"]["count"] == 999  # Mutation in snapshot1 preserved

        # Also verify list/dict replacement works
        snapshot1.append({"id": 2})
        assert len(snapshot2) == 1  # snapshot2 unaffected by append

    def test_idempotency_no_new_events(self, mock_cursor):
        """Test calling get_current_snapshot multiple times with no new events."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Insert initial data
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]
        snapshot1 = compressor.get_current_snapshot()

        # Call again with no new events
        mock_cursor.fetchmany.side_effect = [[]]
        snapshot2 = compressor.get_current_snapshot()

        # Call again with no new events
        mock_cursor.fetchmany.side_effect = [[]]
        snapshot3 = compressor.get_current_snapshot()

        assert snapshot1 == snapshot2 == snapshot3
        assert len(snapshot1) == 1

    def test_no_upsert_columns_tuple_rows(self, mock_cursor):
        """Test get_current_snapshot with NoUpsertColumnsCompressor (tuple mode)."""
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock batch with operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),
            ],
            [],
        ]

        snapshot = compressor.get_current_snapshot()

        assert len(snapshot) == 2
        assert isinstance(snapshot[0], tuple)

    def test_dict_rows_with_upsert_columns(self, mock_cursor):
        """Test get_current_snapshot with dict rows (UpsertColumnsCompressor)."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock dict rows
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10})],
            [ChangeloggedRow(Op.INSERT, {"id": 2, "value": "b", "count": 20})],
            [],
        ]

        snapshot = compressor.get_current_snapshot()

        assert len(snapshot) == 2
        assert isinstance(snapshot[0], dict)
        assert isinstance(snapshot[1], dict)
        assert snapshot[0]["id"] == 1
        assert snapshot[1]["id"] == 2

    def test_dict_rows_without_upsert_columns(self, mock_cursor):
        """Test get_current_snapshot with dict rows (NoUpsertColumnsCompressor)."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock dict rows
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10})],
            [],
        ]

        snapshot = compressor.get_current_snapshot()

        assert len(snapshot) == 1
        assert isinstance(snapshot[0], dict)
        assert snapshot[0]["id"] == 1

    def test_custom_batch_size(self, mock_cursor):
        """Test get_current_snapshot respects custom batch size."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        snapshot = compressor.get_current_snapshot(fetch_batchsize=50)

        # Verify custom batch size was used
        mock_cursor.fetchmany.assert_called_with(50)
        assert len(snapshot) == 1

    def test_default_batch_size(self, mock_cursor):
        """Test get_current_snapshot uses cursor.arraysize by default."""
        mock_cursor.arraysize = 200
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        snapshot = compressor.get_current_snapshot()

        # Verify arraysize was used
        mock_cursor.fetchmany.assert_called_with(200)
        assert len(snapshot) == 1

    def test_snapshots_captures_batchsize_once(self, mock_cursor):
        """Test that snapshots() captures batch size once, not per-yield.

        This test verifies backward compatibility: if fetch_batchsize is None,
        snapshots() should resolve cursor.arraysize once at the start and use
        that value consistently across all yields, even if cursor.arraysize
        is mutated between yields by the caller.
        """
        mock_cursor._statement.traits.upsert_columns = [0]
        mock_cursor.arraysize = 50  # Initial batch size
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        # Track which batch sizes were used in each fetchmany call
        batch_sizes_used = []

        def track_fetchmany(size):
            batch_sizes_used.append(size)
            # Return data on first call, empty on subsequent calls
            if len(batch_sizes_used) == 1:
                return [ChangeloggedRow(Op.INSERT, (1, "a", 10))]
            return []

        mock_cursor.fetchmany.side_effect = track_fetchmany

        # Get first snapshot
        gen = compressor.snapshots()
        first_snapshot = next(gen)
        assert len(first_snapshot) == 1

        # Now mutate cursor.arraysize - this should NOT affect the generator
        mock_cursor.arraysize = 999

        # Reset side_effect for next iteration
        batch_sizes_used.clear()
        mock_cursor.fetchmany.side_effect = track_fetchmany

        # Get second snapshot - it should still use the original batch size (50)
        next(gen)
        assert batch_sizes_used[0] == 50, (
            f"Expected batch size 50 (original), got {batch_sizes_used[0]}. "
            "snapshots() must capture batch size once, not per-yield."
        )

    def test_snapshots_explicit_batchsize_param(self, mock_cursor):
        """Test that snapshots() with explicit fetch_batchsize uses it consistently.

        When an explicit fetch_batchsize is provided, it should be used for all
        yields regardless of cursor.arraysize mutations.
        """
        mock_cursor._statement.traits.upsert_columns = [0]
        mock_cursor.arraysize = 100
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        batch_sizes_used = []

        def track_fetchmany(size):
            batch_sizes_used.append(size)
            if len(batch_sizes_used) <= 2:
                return [ChangeloggedRow(Op.INSERT, (1, "a", 10))]
            return []

        mock_cursor.fetchmany.side_effect = track_fetchmany

        gen = compressor.snapshots(fetch_batchsize=75)

        # First yield
        next(gen)
        first_yield_batch_size = batch_sizes_used[-1]

        # Mutate arraysize
        mock_cursor.arraysize = 200

        # Second yield should still use explicit batchsize (75)
        next(gen)
        second_yield_batch_size = batch_sizes_used[-1]

        assert first_yield_batch_size == 75
        assert second_yield_batch_size == 75
        assert first_yield_batch_size == second_yield_batch_size, (
            "Explicit fetch_batchsize should be used consistently across all yields"
        )

    def test_snapshots_rejects_zero_batchsize(self, mock_cursor):
        """Test that snapshots() rejects fetch_batchsize=0."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(ValueError, match="fetch_batchsize must be positive"):
            next(compressor.snapshots(fetch_batchsize=0))

    def test_snapshots_rejects_negative_batchsize(self, mock_cursor):
        """Test that snapshots() rejects negative fetch_batchsize."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(ValueError, match="fetch_batchsize must be positive"):
            next(compressor.snapshots(fetch_batchsize=-10))

    def test_get_current_snapshot_rejects_zero_batchsize(self, mock_cursor):
        """Test that get_current_snapshot() rejects fetch_batchsize=0."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(ValueError, match="fetch_batchsize must be positive"):
            compressor.get_current_snapshot(fetch_batchsize=0)

    def test_get_current_snapshot_rejects_negative_batchsize(self, mock_cursor):
        """Test that get_current_snapshot() rejects negative fetch_batchsize."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(ValueError, match="fetch_batchsize must be positive"):
            compressor.get_current_snapshot(fetch_batchsize=-5)

    def test_snapshots_rejects_zero_cursor_arraysize(self, mock_cursor):
        """Test that snapshots() rejects when cursor.arraysize is zero."""
        mock_cursor.arraysize = 0
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            ValueError, match="batch size must be positive, got 0.*cursor.arraysize"
        ):
            next(compressor.snapshots())

    def test_snapshots_rejects_negative_cursor_arraysize(self, mock_cursor):
        """Test that snapshots() rejects when cursor.arraysize is negative."""
        mock_cursor.arraysize = -5
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            ValueError, match="batch size must be positive, got -5.*cursor.arraysize"
        ):
            next(compressor.snapshots())

    def test_get_current_snapshot_rejects_zero_cursor_arraysize(self, mock_cursor):
        """Test that get_current_snapshot() rejects when cursor.arraysize is zero."""
        mock_cursor.arraysize = 0
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            ValueError, match="batch size must be positive, got 0.*cursor.arraysize"
        ):
            compressor.get_current_snapshot()

    def test_get_current_snapshot_rejects_negative_cursor_arraysize(self, mock_cursor):
        """Test that get_current_snapshot() rejects when cursor.arraysize is negative."""
        mock_cursor.arraysize = -10
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            ValueError, match="batch size must be positive, got -10.*cursor.arraysize"
        ):
            compressor.get_current_snapshot()
