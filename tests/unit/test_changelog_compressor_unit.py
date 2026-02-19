"""Unit tests for changelog compressor module."""

from unittest.mock import MagicMock

import pytest

from confluent_sql.changelog import ChangeloggedRow, RawChangelogProcessor
from confluent_sql.changelog_compressor import (
    NoUpsertColumnsDictCompressor,
    NoUpsertColumnsTupleCompressor,
    UpsertColumnsDictCompressor,
    UpsertColumnsTupleCompressor,
    create_changelog_compressor,
)
from confluent_sql.cursor import Cursor
from confluent_sql.exceptions import InterfaceError
from confluent_sql.statement import Op, Schema, Statement, Traits


@pytest.fixture
def mock_cursor():
    """Create a mock cursor for testing compressors."""
    cursor = MagicMock(spec=Cursor)
    cursor.arraysize = 100
    cursor.as_dict = False  # Use the public property
    cursor.returns_changelog = True

    # Mock statement with schema and traits
    statement = MagicMock(spec=Statement)
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

    return cursor


@pytest.mark.unit
class TestUpsertColumnsTupleCompressor:
    """Tests for UpsertColumnsTupleCompressor."""

    def test_insert_operations(self, mock_cursor):
        """Test handling of INSERT operations."""
        mock_cursor._statement.traits.upsert_columns = [0]  # id is the key
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return INSERT operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
            ],
            [],  # End of results
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)

    def test_update_operations(self, mock_cursor):
        """Test handling of UPDATE_BEFORE and UPDATE_AFTER operations."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with updates
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 15)),  # Update count
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 15)

    def test_delete_operations(self, mock_cursor):
        """Test handling of DELETE operations."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with delete
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (2, "b", 20)

    def test_compound_key(self, mock_cursor):
        """Test with compound upsert columns."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # id and value are keys
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with compound key operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "b", 20)),  # Same id, different value
                ChangeloggedRow(Op.INSERT, (2, "a", 30)),  # Different id, same value
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 3
        # Order preserved
        assert (1, "a", 10) in snapshot
        assert (1, "b", 20) in snapshot
        assert (2, "a", 30) in snapshot

    def test_deep_copy_returned(self, mock_cursor):
        """Test that get_snapshot returns a deep copy."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, ["nested", "list"], 10))],
            [],
        ]

        snapshot1 = compressor.get_snapshot()

        # Reset fetchmany for second call
        mock_cursor.fetchmany.side_effect = [
            [],
        ]  # No new results

        snapshot2 = compressor.get_snapshot()

        # Modify the first snapshot
        # Cast to list since we know it's a list from the test setup
        nested_list = snapshot1[0][1]
        if isinstance(nested_list, list):
            nested_list.append("modified")

        # Second snapshot should not be affected
        assert snapshot2[0][1] == ["nested", "list"]


@pytest.mark.unit
class TestUpsertColumnsDictCompressor:
    """Tests for UpsertColumnsDictCompressor."""

    def test_dict_operations(self, mock_cursor):
        """Test compressor with dict results."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with dict results
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b", "count": 15}),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b", "count": 15}

    def test_key_extraction_from_dict(self, mock_cursor):
        """Test key extraction from dictionary rows."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [1, 2]  # value and count are keys
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.INSERT, {"id": 2, "value": "a", "count": 10}),  # Same key
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Should only have one row since keys are the same
        assert len(snapshot) == 1
        assert snapshot[0]["id"] == 2  # Last one wins

    def test_key_extraction_from_dict_uses_schema(self, mock_cursor):
        """Test that key extraction from dict correctly uses schema columns."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]  # id is the key
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with dict operations
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a", "count": 10}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b", "count": 20}),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Verify key extraction worked correctly (used schema to map dict to indices)
        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b", "count": 20}


@pytest.mark.unit
class TestNoUpsertColumnsTupleCompressor:
    """Tests for NoUpsertColumnsTupleCompressor."""

    def test_without_keys(self, mock_cursor):
        """Test compressor without upsert columns."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate allowed
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 2  # Both rows kept
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (1, "a", 10)

    def test_scan_based_updates(self, mock_cursor):
        """Test scan-based UPDATE operations."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 15)  # Updated
        assert snapshot[1] == (2, "b", 20)  # Unchanged

    def test_scan_based_delete(self, mock_cursor):
        """Test scan-based DELETE operations."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Deletes the last one
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1  # One remains
        assert snapshot[0] == (1, "a", 10)

    def test_position_adjustment_after_delete(self, mock_cursor):
        """Test that UPDATE_BEFORE must be immediately followed by UPDATE_AFTER."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()


@pytest.mark.unit
class TestNoUpsertColumnsDictCompressor:
    """Tests for NoUpsertColumnsDictCompressor."""

    def test_dict_without_keys(self, mock_cursor):
        """Test dict compressor without upsert columns."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a"}),
                ChangeloggedRow(Op.UPDATE_BEFORE, {"id": 1, "value": "a"}),
                ChangeloggedRow(Op.UPDATE_AFTER, {"id": 1, "value": "b"}),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == {"id": 1, "value": "b"}


@pytest.mark.unit
class TestCompressorValidation:
    """Test validation in ChangelogCompressor and its subclasses."""

    def test_all_compressors_require_changelog_cursor(self, mock_cursor):
        """Test that all compressor types raise InterfaceError when cursor.returns_changelog is False."""
        # Set cursor to not return changelog
        mock_cursor.returns_changelog = False

        # Test UpsertColumnsTupleCompressor
        mock_cursor._statement.traits.upsert_columns = [0]
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Test UpsertColumnsDictCompressor
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Test NoUpsertColumnsTupleCompressor
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Test NoUpsertColumnsDictCompressor
        with pytest.raises(
            InterfaceError,
            match="ChangelogCompressor can only be created for streaming non-append-only queries",
        ):
            NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_upsert_columns_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without upsert columns."""
        # Remove upsert columns from statement
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(
            InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"
        ):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"
        ):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_traits_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without traits."""
        # Remove traits entirely
        mock_cursor._statement.traits = None

        with pytest.raises(
            InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"
        ):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

    def test_all_compressors_require_schema(self, mock_cursor):
        """Test that all compressor types raise InterfaceError when statement has no schema."""
        # Remove schema from statement
        mock_cursor._statement.schema = None

        # Test with upsert columns (though it won't get that far)
        mock_cursor._statement.traits.upsert_columns = [0]

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Test without upsert columns
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(
            InterfaceError, match="ChangelogCompressor requires a statement with a schema"
        ):
            NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)


@pytest.mark.unit
class TestFactoryFunction:
    """Tests for the create_changelog_compressor factory function."""

    def test_factory_selects_correct_compressor(self, mock_cursor):
        """Test that the factory function selects the correct compressor class."""
        # Test with upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = [0]

        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, UpsertColumnsTupleCompressor)

        # Test with upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, UpsertColumnsDictCompressor)

        # Test without upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = None
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, NoUpsertColumnsTupleCompressor)

        # Test without upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = create_changelog_compressor(mock_cursor, mock_cursor._statement)
        assert isinstance(compressor, NoUpsertColumnsDictCompressor)

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

    def test_compressor_selection(self, mock_cursor):
        """Test that correct compressor class is selected based on configuration."""
        # Test with upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = [0]

        # Mock the method on cursor
        mock_cursor.changelog_compressor = Cursor.changelog_compressor.__get__(mock_cursor, Cursor)

        compressor = mock_cursor.changelog_compressor()
        assert isinstance(compressor, UpsertColumnsTupleCompressor)

        # Test with upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = mock_cursor.changelog_compressor()
        assert isinstance(compressor, UpsertColumnsDictCompressor)

        # Test without upsert columns and tuples
        mock_cursor.as_dict = False
        mock_cursor._statement.traits.upsert_columns = None
        compressor = mock_cursor.changelog_compressor()
        assert isinstance(compressor, NoUpsertColumnsTupleCompressor)

        # Test without upsert columns and dicts
        mock_cursor.as_dict = True
        compressor = mock_cursor.changelog_compressor()
        assert isinstance(compressor, NoUpsertColumnsDictCompressor)


@pytest.mark.unit
class TestBatchSize:
    """Tests for batch size handling."""

    def test_custom_batch_size(self, mock_cursor):
        """Test using custom batch size for fetching."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        # Use custom batch size
        snapshot = compressor.get_snapshot(fetch_batchsize=50)

        # Verify fetchmany was called with custom size
        mock_cursor.fetchmany.assert_called_with(50)
        assert len(snapshot) == 1

    def test_default_batch_size(self, mock_cursor):
        """Test using cursor's arraysize as default."""
        mock_cursor.arraysize = 200
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Verify fetchmany was called with cursor's arraysize
        mock_cursor.fetchmany.assert_called_with(200)
        assert len(snapshot) == 1


@pytest.mark.unit
class TestCloseMethod:
    """Tests for the close() method."""

    def test_close_calls_cursor_close(self, mock_cursor):
        """Test that close() calls the cursor's close method."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],
        ]
        compressor.get_snapshot()

        # Close the compressor
        compressor.close()

        # Verify cursor.close() was called
        mock_cursor.close.assert_called_once()

    def test_close_clears_upsert_compressor_state(self, mock_cursor):
        """Test that close() clears internal state for UpsertColumnsCompressor."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
            ],
            [],
        ]
        compressor.get_snapshot()

        # Verify data exists
        assert len(compressor._rows_by_key) == 2

        # Close the compressor
        compressor.close()

        # Verify internal state is cleared
        assert len(compressor._rows_by_key) == 0

    def test_close_clears_no_upsert_compressor_state(self, mock_cursor):
        """Test that close() clears internal state for NoUpsertColumnsCompressor."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
            ],
            [],
        ]
        compressor.get_snapshot()

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
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Add some data
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10})],
            [],
        ]
        compressor.get_snapshot()

        # Close the compressor
        compressor.close()

        # Verify cursor.close() was called and state is cleared
        mock_cursor.close.assert_called_once()
        assert len(compressor._rows_by_key) == 0

    def test_close_idempotent(self, mock_cursor):
        """Test that close() can be called multiple times safely."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with missing UPDATE_AFTER
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                # UPDATE_AFTER is missing
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Original row should remain unchanged
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

    def test_update_after_without_before(self, mock_cursor):
        """Test bare UPDATE_AFTER requires UPDATE_BEFORE in NoUpsertColumnsCompressor."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_multiple_batches(self, mock_cursor):
        """Test fetching across multiple batches."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return multiple batches
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],  # First batch
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],  # Second batch
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Third batch
            [],  # End of results
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 3
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)
        assert snapshot[2] == (3, "c", 30)

    def test_empty_results(self, mock_cursor):
        """Test with no results."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return empty
        mock_cursor.fetchmany.side_effect = [[]]

        snapshot = compressor.get_snapshot()

        assert snapshot == []

    def test_overwriting_pending_update(self, mock_cursor):
        """Test that UPDATE_BEFORE must be followed by UPDATE_AFTER immediately."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()


@pytest.mark.unit
class TestUpsertColumnsCompressorErrorCases:
    """Tests for error cases in UpsertColumnsCompressor when keys are not found."""

    def test_update_before_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: \(999,\)",
        ):
            compressor.get_snapshot()

    def test_update_after_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_before_after_delete_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE after DELETE raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_after_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER without any prior INSERT raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_before_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: \(1, 'z'\)",
        ):
            compressor.get_snapshot()

    def test_update_after_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: \(2, 'a'\)",
        ):
            compressor.get_snapshot()

    def test_update_before_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: \(999,\)",
        ):
            compressor.get_snapshot()

    def test_update_after_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_AFTER for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_with_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that DELETE for a non-existent key raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that DELETE without any prior INSERT raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_twice_same_key_raises_interface_error(self, mock_cursor):
        """Test that deleting the same key twice raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_with_compound_key_not_found_raises_interface_error(self, mock_cursor):
        """Test that DELETE with compound key not found raises InterfaceError."""
        mock_cursor._statement.traits.upsert_columns = [0, 1]  # Compound key
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_with_dict_rows_nonexistent_key_raises_interface_error(self, mock_cursor):
        """Test that DELETE for dict rows with non-existent key raises InterfaceError."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_successful_delete_does_not_raise_error(self, mock_cursor):
        """Test that DELETE for an existing key works correctly."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with successful DELETE
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Should succeed
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Verify delete worked
        assert len(snapshot) == 1
        assert snapshot[0] == (2, "b", 20)


@pytest.mark.unit
class TestNoUpsertColumnsCompressorErrorCases:
    """Tests for error cases in NoUpsertColumnsCompressor when rows are not found."""

    def test_update_before_with_nonexistent_row_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for a non-existent row raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_after_without_update_before_raises_interface_error(self, mock_cursor):
        """Test that bare UPDATE_AFTER without UPDATE_BEFORE raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_before_after_delete_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE after DELETE raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_with_nonexistent_row_raises_interface_error(self, mock_cursor):
        """Test that DELETE for a non-existent row raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_without_prior_insert_raises_interface_error(self, mock_cursor):
        """Test that DELETE without any prior INSERT raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_twice_same_row_raises_interface_error(self, mock_cursor):
        """Test that deleting the same row twice raises InterfaceError."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_before_finds_most_recent_duplicate(self, mock_cursor):
        """Test that UPDATE_BEFORE finds the most recent duplicate row."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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

        snapshot = compressor.get_snapshot()

        # Should have two rows: one unchanged, one updated
        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)  # First one unchanged
        assert snapshot[1] == (1, "a", 20)  # Second one updated

    def test_delete_removes_most_recent_duplicate(self, mock_cursor):
        """Test that DELETE removes the most recent duplicate row."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with duplicate rows
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),  # Duplicate
                ChangeloggedRow(Op.DELETE, (1, "a", 10)),  # Deletes most recent
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # Should have one row remaining
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

    def test_update_before_with_dict_rows_nonexistent_raises_interface_error(self, mock_cursor):
        """Test that UPDATE_BEFORE for dict rows with non-existent row raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_update_after_without_before_dict_rows_raises_interface_error(self, mock_cursor):
        """Test that bare UPDATE_AFTER for dict rows raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_with_dict_rows_nonexistent_raises_interface_error(self, mock_cursor):
        """Test that DELETE for dict rows with non-existent row raises InterfaceError."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_successful_operations_do_not_raise_errors(self, mock_cursor):
        """Test that valid operation sequences work correctly."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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

        snapshot = compressor.get_snapshot()

        # Verify operations worked correctly
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 15)


@pytest.mark.unit
class TestUpsertColumnsCompressorUpdateSequencing:
    """Tests for UPDATE_BEFORE/UPDATE_AFTER sequencing in UpsertColumnsCompressor."""

    def test_bare_update_after_with_existing_key_succeeds(self, mock_cursor):
        """Test that bare UPDATE_AFTER works when key exists."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # Bare UPDATE_AFTER - no UPDATE_BEFORE
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)

    def test_insert_after_update_before_raises_error(self, mock_cursor):
        """Test that INSERT after UPDATE_BEFORE raises error."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_after_update_before_raises_error(self, mock_cursor):
        """Test that DELETE after UPDATE_BEFORE raises error."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_paired_update_before_after_succeeds(self, mock_cursor):
        """Test that UPDATE_BEFORE immediately followed by UPDATE_AFTER works."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)


@pytest.mark.unit
class TestNoUpsertColumnsCompressorUpdateSequencing:
    """Tests for UPDATE_BEFORE/UPDATE_AFTER sequencing in NoUpsertColumnsCompressor."""

    def test_bare_update_after_requires_update_before(self, mock_cursor):
        """Test that bare UPDATE_AFTER requires UPDATE_BEFORE."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_insert_after_update_before_raises_error(self, mock_cursor):
        """Test that INSERT after UPDATE_BEFORE raises error."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_delete_after_update_before_raises_error(self, mock_cursor):
        """Test that DELETE after UPDATE_BEFORE raises error."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            compressor.get_snapshot()

    def test_paired_update_before_after_succeeds(self, mock_cursor):
        """Test that UPDATE_BEFORE immediately followed by UPDATE_AFTER works."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)


@pytest.mark.unit
class TestCursorClearChangelogBuffer:
    """Tests for Cursor.clear_changelog_buffer() method."""

    def test_clear_changelog_buffer_calls_processor_clear_buffer(self, mock_cursor):
        """Test that clear_changelog_buffer() delegates to processor.clear_buffer()."""
        # Bind the real method to the mock cursor
        mock_cursor.clear_changelog_buffer = Cursor.clear_changelog_buffer.__get__(
            mock_cursor, Cursor
        )

        # Create a mock processor
        mock_processor = MagicMock(spec=RawChangelogProcessor)
        mock_cursor._changelog_processor = mock_processor

        # Call clear_changelog_buffer
        mock_cursor.clear_changelog_buffer()

        # Verify it called clear_buffer on the processor
        mock_processor.clear_buffer.assert_called_once()

    def test_clear_changelog_buffer_no_op_when_no_processor(self, mock_cursor):
        """Test that clear_changelog_buffer() is a no-op when processor is None."""

        mock_cursor._changelog_processor = None

        # Should not raise an error even if processor is None, just do nothing
        mock_cursor.clear_changelog_buffer()


@pytest.mark.unit
class TestGetSnapshotCallsClearBuffer:
    """Tests that get_snapshot() calls clear_changelog_buffer() after fetching."""

    def test_upsert_columns_tuple_compressor_calls_clear_buffer(self, mock_cursor):
        """Test UpsertColumnsTupleCompressor.get_snapshot() calls clear_changelog_buffer()."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return some data then empty
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
            ],
            [],  # Empty batch signals end
        ]

        # Execute get_snapshot
        snapshot = compressor.get_snapshot()

        # Verify clear_changelog_buffer was called after fetchmany returned empty
        mock_cursor.clear_changelog_buffer.assert_called_once()

        # Verify snapshot is correct
        assert len(snapshot) == 2

    def test_upsert_columns_dict_compressor_calls_clear_buffer(self, mock_cursor):
        """Test UpsertColumnsDictCompressor.get_snapshot() calls clear_changelog_buffer()."""
        mock_cursor.as_dict = True
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return some data then empty
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        mock_cursor.clear_changelog_buffer.assert_called_once()
        assert len(snapshot) == 1

    def test_no_upsert_columns_tuple_compressor_calls_clear_buffer(self, mock_cursor):
        """Test NoUpsertColumnsTupleCompressor.get_snapshot() calls clear_changelog_buffer()."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        mock_cursor.clear_changelog_buffer.assert_called_once()
        assert len(snapshot) == 1

    def test_no_upsert_columns_dict_compressor_calls_clear_buffer(self, mock_cursor):
        """Test NoUpsertColumnsDictCompressor.get_snapshot() calls clear_changelog_buffer()."""
        mock_cursor.as_dict = True
        compressor = NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, {"id": 1, "value": "a", "count": 10}),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        mock_cursor.clear_changelog_buffer.assert_called_once()
        assert len(snapshot) == 1

    def test_clear_buffer_called_only_when_fetchmany_returns_empty(self, mock_cursor):
        """Test that clear_changelog_buffer() is only called when fetchmany returns []."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return data in multiple batches
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],
            [],  # Empty signals end
        ]

        snapshot = compressor.get_snapshot()

        # Should be called exactly once, after the empty batch
        mock_cursor.clear_changelog_buffer.assert_called_once()
        assert len(snapshot) == 3

    def test_clear_buffer_not_called_on_exception(self, mock_cursor):
        """Test clear_changelog_buffer() is not called if exception occurs during fetch."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to raise an exception
        mock_cursor.fetchmany.side_effect = RuntimeError("Fetch failed")

        with pytest.raises(RuntimeError, match="Fetch failed"):
            compressor.get_snapshot()

        # Should not have been called because we never reached the empty batch
        mock_cursor.clear_changelog_buffer.assert_not_called()
