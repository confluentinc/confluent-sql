import pytest

from confluent_sql.execution_mode import ExecutionMode


@pytest.mark.unit
class TestExecutionModeUnit:
    @pytest.mark.parametrize(
        "mode, expected_is_ddl",
        [
            (ExecutionMode.SNAPSHOT, False),
            (ExecutionMode.STREAMING_QUERY, False),
            (ExecutionMode.SNAPSHOT_DDL, True),
            (ExecutionMode.STREAMING_DDL, True),
        ],
    )
    def test_is_ddl(self, mode: ExecutionMode, expected_is_ddl: bool):
        assert mode.is_ddl == expected_is_ddl

    @pytest.mark.parametrize(
        "mode, expected_is_snapshot",
        [
            (ExecutionMode.SNAPSHOT, True),
            (ExecutionMode.STREAMING_QUERY, False),
            (ExecutionMode.SNAPSHOT_DDL, True),
            (ExecutionMode.STREAMING_DDL, False),
        ],
    )
    def test_is_snapshot(self, mode: ExecutionMode, expected_is_snapshot: bool):
        assert mode.is_snapshot == expected_is_snapshot
