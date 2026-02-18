"""Unit tests for changelog compressor module."""

import pytest
from unittest.mock import MagicMock

from confluent_sql.changelog import ChangeloggedRow
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
    schema.columns = [
        MagicMock(name="id"),
        MagicMock(name="value"),
        MagicMock(name="count"),
    ]
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
        mock_cursor.fetchmany.side_effect = [[], ]  # No new results

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
        """Test that pending positions are adjusted after deletions."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.INSERT, (2, "b", 20)),
                ChangeloggedRow(Op.INSERT, (3, "c", 30)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (3, "c", 30)),  # Mark position 2
                ChangeloggedRow(Op.DELETE, (2, "b", 20)),  # Delete position 1
                ChangeloggedRow(Op.UPDATE_AFTER, (3, "c", 35)),  # Should update correctly
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (3, "c", 35)  # Updated correctly after delete


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
        with pytest.raises(InterfaceError, match="ChangelogCompressor can only be created for streaming non-append-only queries"):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Test UpsertColumnsDictCompressor
        with pytest.raises(InterfaceError, match="ChangelogCompressor can only be created for streaming non-append-only queries"):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Test NoUpsertColumnsTupleCompressor
        with pytest.raises(InterfaceError, match="ChangelogCompressor can only be created for streaming non-append-only queries"):
            NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Test NoUpsertColumnsDictCompressor
        with pytest.raises(InterfaceError, match="ChangelogCompressor can only be created for streaming non-append-only queries"):
            NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_upsert_columns_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without upsert columns."""
        # Remove upsert columns from statement
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

    def test_upsert_compressor_without_traits_raises(self, mock_cursor):
        """Test that UpsertColumnsCompressor raises InterfaceError without traits."""
        # Remove traits entirely
        mock_cursor._statement.traits = None

        with pytest.raises(InterfaceError, match="UpsertColumnsCompressor requires a statement with upsert columns"):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

    def test_all_compressors_require_schema(self, mock_cursor):
        """Test that all compressor types raise InterfaceError when statement has no schema."""
        # Remove schema from statement
        mock_cursor._statement.schema = None

        # Test with upsert columns (though it won't get that far)
        mock_cursor._statement.traits.upsert_columns = [0]

        with pytest.raises(InterfaceError, match="ChangelogCompressor requires a statement with a schema"):
            UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(InterfaceError, match="ChangelogCompressor requires a statement with a schema"):
            UpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

        # Test without upsert columns
        mock_cursor._statement.traits.upsert_columns = None

        with pytest.raises(InterfaceError, match="ChangelogCompressor requires a statement with a schema"):
            NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        with pytest.raises(InterfaceError, match="ChangelogCompressor requires a statement with a schema"):
            NoUpsertColumnsDictCompressor(mock_cursor, mock_cursor._statement)

    def test_validation_order_changelog_before_schema(self, mock_cursor):
        """Test that changelog validation happens before schema validation."""
        # Set cursor to not return changelog AND remove schema
        mock_cursor.returns_changelog = False
        mock_cursor._statement.schema = None

        # Should get the changelog error first, not the schema error
        with pytest.raises(InterfaceError, match="ChangelogCompressor can only be created for streaming non-append-only queries"):
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

        with pytest.raises(InterfaceError, match="can only be created for streaming non-append-only"):
            create_changelog_compressor(mock_cursor, mock_cursor._statement)


@pytest.mark.unit
class TestChangelogCompressorCreation:
    """Tests for creating compressors from cursor."""

    def test_create_compressor_for_non_changelog(self, mock_cursor):
        """Test that creating compressor for non-changelog query raises error."""
        mock_cursor.returns_changelog = False

        # Bind the real method to the mock
        mock_cursor.changelog_compressor = Cursor.changelog_compressor.__get__(mock_cursor, Cursor)

        with pytest.raises(InterfaceError, match="can only be created for streaming non-append-only"):
            mock_cursor.changelog_compressor()

    def test_create_compressor_without_statement(self, mock_cursor):
        """Test that creating compressor without a statement raises error."""
        mock_cursor._statement = None

        # Bind the real method to the mock
        mock_cursor.changelog_compressor = Cursor.changelog_compressor.__get__(mock_cursor, Cursor)

        with pytest.raises(InterfaceError, match="Cannot create changelog compressor without a statement"):
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
        assert len(compressor._rows) == 2
        assert len(compressor._pending_update_positions) == 1

        # Close the compressor
        compressor.close()

        # Verify internal state is cleared
        assert len(compressor._rows) == 0
        assert len(compressor._pending_update_positions) == 0

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
        assert len(compressor._rows) == 0

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
        """Test UPDATE_AFTER without UPDATE_BEFORE."""
        compressor = NoUpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                # UPDATE_BEFORE is missing
                ChangeloggedRow(Op.UPDATE_AFTER, (2, "b", 20)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        # UPDATE_AFTER treated as INSERT when no matching UPDATE_BEFORE
        assert len(snapshot) == 2
        assert snapshot[0] == (1, "a", 10)
        assert snapshot[1] == (2, "b", 20)

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
        """Test that subsequent UPDATE_BEFORE overwrites pending position."""
        mock_cursor._statement.traits.upsert_columns = [0]
        compressor = UpsertColumnsTupleCompressor(mock_cursor, mock_cursor._statement)

        # Mock fetchmany with multiple UPDATE_BEFOREs for same key
        mock_cursor.fetchmany.side_effect = [
            [
                ChangeloggedRow(Op.INSERT, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),
                ChangeloggedRow(Op.UPDATE_BEFORE, (1, "a", 10)),  # Overwrites previous
                ChangeloggedRow(Op.UPDATE_AFTER, (1, "a", 20)),
            ],
            [],
        ]

        snapshot = compressor.get_snapshot()

        assert len(snapshot) == 1
        assert snapshot[0] == (1, "a", 20)