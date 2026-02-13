from enum import Enum


class ExecutionMode(Enum):
    """Controls statement execution and result handling behavior."""

    SNAPSHOT = "snapshot"
    """Submit the statement as a snapshot query -- point in time results, bounded result set."""

    STREAMING_QUERY = "streaming_query"
    """Submit the statement as a streaming query -- possibly(probably) unbounded result set."""

    SNAPSHOT_DDL = "snapshot_ddl"
    """Submit the statement as a snapshot DDL -- point in time schema change. Any queries done
    as part of this DDL will be executed as snapshot queries. No results will be returned."""

    STREAMING_DDL = "streaming_ddl"
    """Submit the statement as a streaming DDL -- ongoing schema change. Any queries done as part
    of this DDL will be executed as long-lived streaming queries. No results will be returned."""

    @property
    def is_ddl(self) -> bool:
        """Check if the execution mode is for DDL statements."""
        return self in {ExecutionMode.SNAPSHOT_DDL, ExecutionMode.STREAMING_DDL}

    @property
    def is_snapshot(self) -> bool:
        """Check if the execution mode is for snapshot statements."""
        return self in {ExecutionMode.SNAPSHOT, ExecutionMode.SNAPSHOT_DDL}

    @property
    def is_streaming(self) -> bool:
        """Check if the execution mode is for streaming statements."""
        return self in {ExecutionMode.STREAMING_QUERY, ExecutionMode.STREAMING_DDL}
