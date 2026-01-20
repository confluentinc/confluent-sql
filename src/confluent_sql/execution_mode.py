from enum import Enum


class ExecutionMode(Enum):
    """Controls statement execution and result handling behavior."""

    SNAPSHOT = "snapshot"
    """Submit the statement as a sna."""
    STREAMING_QUERY = "streaming_query"
    SNAPSHOT_DDL = "snapshot_ddl"
    STREAMING_DDL = "streaming_ddl"

    @property
    def is_ddl(self) -> bool:
        """Check if the execution mode is for DDL statements."""
        return self in {ExecutionMode.SNAPSHOT_DDL, ExecutionMode.STREAMING_DDL}

    @property
    def is_streaming(self) -> bool:
        """Check if the execution mode is for streaming statements."""
        return self in {ExecutionMode.STREAMING_QUERY, ExecutionMode.STREAMING_DDL}

    @property
    def is_snapshot(self) -> bool:
        """Check if the execution mode is for snapshot statements."""
        return self in {ExecutionMode.SNAPSHOT, ExecutionMode.SNAPSHOT_DDL}
