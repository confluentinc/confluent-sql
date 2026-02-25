"""Unit tests for changelog compressor module."""

from unittest.mock import MagicMock

import pytest

from confluent_sql.changelog_compressor import (
    NoUpsertColumnsDictCompressor,
    NoUpsertColumnsTupleCompressor,
    UpsertColumnsDictCompressor,
    UpsertColumnsTupleCompressor,
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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot1 = next(compressor.snapshots())

        # Reset fetchmany for second call
        mock_cursor.fetchmany.side_effect = [
            [],
        ]  # No new results

        snapshot2 = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())


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
        snapshot = next(compressor.snapshots(fetch_batchsize=50))

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
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
        next(compressor.snapshots())

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
        compressor = UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

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

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 3
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)
        assert snapshot[2] == (3, "c", 30)

    def test_empty_results(self, mock_cursor):
        """Test with no results."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany to return empty
        mock_cursor.fetchmany.side_effect = [[]]

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())


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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(999,\)",
        ):
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(1, 'z'\)",
        ):
            next(compressor.snapshots())

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
            match=r"Received UPDATE_AFTER for a key that does not exist in current state: "
            r"\(2, 'a'\)",
        ):
            next(compressor.snapshots())

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
            match=r"Received UPDATE_BEFORE for a key that does not exist in current state: "
            r"\(999,\)",
        ):
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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
            next(compressor.snapshots())

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

        snapshot = next(compressor.snapshots())

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)


@pytest.mark.unit
class TestSnapshotsGeneratorTermination:
    """Tests for snapshots() generator termination behavior."""

    def test_generator_terminates_when_may_have_results_becomes_false(self, mock_cursor):
        """Test snapshots() raises StatementStoppedError when may_have_results is False."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock may_have_results to change after first snapshot
        call_count = 0

        def may_have_results_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count <= 1  # True first time, False second time

        type(mock_cursor).may_have_results = property(lambda _: may_have_results_side_effect())

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],
            [],  # End of first snapshot
        ]

        # Consume the generator and expect StatementStoppedError
        gen = compressor.snapshots()
        snapshot = next(gen)
        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 10)

        # Next iteration should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        # Verify exception attributes
        assert exc_info.value.statement_name == "test-statement"
        assert exc_info.value.statement is mock_cursor.statement

    def test_generator_yields_multiple_snapshots(self, mock_cursor):
        """Test snapshots() yields multiple snapshots then raises StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Track which snapshot we're on
        snapshots_yielded = [0]  # Use list to allow modification in nested function

        def may_have_results_side_effect():
            # Allow 3 snapshots to be yielded
            return snapshots_yielded[0] < 3

        type(mock_cursor).may_have_results = property(lambda _: may_have_results_side_effect())

        # Mock fetchmany to return data then empty for each snapshot
        # Pattern: data, [], data, [], data, []
        mock_cursor.fetchmany.side_effect = [
            [ChangeloggedRow(Op.INSERT, (1, "a", 10))],  # Snapshot 1 data
            [],  # End snapshot 1
            [ChangeloggedRow(Op.INSERT, (2, "b", 20))],  # Snapshot 2 data
            [],  # End snapshot 2
            [ChangeloggedRow(Op.INSERT, (3, "c", 30))],  # Snapshot 3 data
            [],  # End snapshot 3
        ]

        # Track when snapshots are yielded to update may_have_results
        gen = compressor.snapshots()

        snapshot1 = next(gen)
        snapshots_yielded[0] = 1
        assert len(snapshot1) == 1
        assert snapshot1[0] == (1, "a", 10)

        snapshot2 = next(gen)
        snapshots_yielded[0] = 2
        assert len(snapshot2) == 2
        assert (1, "a", 10) in snapshot2
        assert (2, "b", 20) in snapshot2

        snapshot3 = next(gen)
        snapshots_yielded[0] = 3
        assert len(snapshot3) == 3
        assert (1, "a", 10) in snapshot3
        assert (2, "b", 20) in snapshot3
        assert (3, "c", 30) in snapshot3

        # Next call should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_no_events(self, mock_cursor):
        """Test generator raises StatementStoppedError when may_have_results=False."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock may_have_results to be False immediately
        mock_cursor.may_have_results = False

        # Generator should raise StatementStoppedError immediately
        with pytest.raises(StatementStoppedError) as exc_info:
            next(compressor.snapshots())

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_empty_snapshots(self, mock_cursor):
        """Test generator yields empty snapshots then raises StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Track snapshots
        snapshot_count = 0

        def may_have_results_side_effect():
            nonlocal snapshot_count
            return snapshot_count < 2

        type(mock_cursor).may_have_results = property(lambda _: may_have_results_side_effect())

        # Mock fetchmany to return empty batches
        def fetchmany_side_effect(_size):
            nonlocal snapshot_count
            snapshot_count += 1
            return []

        mock_cursor.fetchmany.side_effect = lambda size: fetchmany_side_effect(size)  # noqa: ARG005

        # Consume the generator
        gen = compressor.snapshots()
        snapshot1 = next(gen)
        snapshot2 = next(gen)

        # Should yield two empty snapshots
        assert snapshot1 == []
        assert snapshot2 == []

        # Next should raise StatementStoppedError
        with pytest.raises(StatementStoppedError) as exc_info:
            next(gen)

        assert exc_info.value.statement_name == "test-statement"

    def test_generator_with_trailing_events_before_termination(self, mock_cursor):
        """Test generator processes all events before raising StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Track fetchmany call count
        call_count = 0

        def may_have_results_side_effect():
            nonlocal call_count
            # True until we've completed 2 snapshots (4 fetchmany calls)
            return call_count < 4

        type(mock_cursor).may_have_results = property(lambda _: may_have_results_side_effect())

        # Mock fetchmany to return data across two snapshots
        def fetchmany_side_effect(_size):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First batch: initial data
                return [
                    ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                    ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ]
            elif call_count == 2:
                # End of first snapshot
                return []
            elif call_count == 3:
                # Second batch: trailing events
                return [ChangeloggedRow(Op.INSERT, (3, "c", 30))]
            else:
                # End of second snapshot and any subsequent calls
                return []

        mock_cursor.fetchmany.side_effect = lambda size: fetchmany_side_effect(size)  # noqa: ARG005

        # Consume the generator
        gen = compressor.snapshots()
        snapshot1 = next(gen)
        snapshot2 = next(gen)

        # Should have two snapshots with accumulated state
        assert len(snapshot1) == 2  # First snapshot has 2 rows
        assert len(snapshot2) == 3  # Second snapshot has 3 rows (accumulated)

        # Next should raise StatementStoppedError
        with pytest.raises(StatementStoppedError):
            next(gen)

    def test_generator_terminates_cleanly_on_for_loop(self, mock_cursor):
        """Test that for loop over snapshots() raises StatementStoppedError."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Track fetchmany call count
        call_count = 0

        def may_have_results_side_effect():
            nonlocal call_count
            # True until we've completed 2 snapshots (4 fetchmany calls)
            return call_count < 4

        type(mock_cursor).may_have_results = property(lambda _: may_have_results_side_effect())

        def fetchmany_side_effect(_size):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [ChangeloggedRow(Op.INSERT, (1, "x", 10))]
            elif call_count == 2:
                return []  # End of first snapshot
            elif call_count == 3:
                return [ChangeloggedRow(Op.INSERT, (2, "y", 20))]
            else:
                return []  # End of second snapshot

        mock_cursor.fetchmany.side_effect = lambda size: fetchmany_side_effect(size)  # noqa: ARG005

        # Use for loop pattern with exception catching
        snapshots_seen = []
        with pytest.raises(StatementStoppedError):
            for snapshot in compressor.snapshots():
                snapshots_seen.append(snapshot)

        # Should have iterated exactly twice before exception
        assert len(snapshots_seen) == 2
