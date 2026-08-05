"""
Confluent SQL - DB-API v2 compliant driver for Confluent SQL.

This module provides a DB-API v2 compliant interface for connecting to and
executing SQL queries against Confluent SQL services.
"""

from .changelog_compressor import ChangelogCompressor
from .connection import Connection, connect
from .connectors import (
    Connector,
    ConnectorSpec,
    ConnectorState,
    ConnectorStatus,
    TaskStatus,
)
from .cursor import Cursor
from .exceptions import (
    ComputePoolExhaustedError,
    ConnectorAlreadyExistsError,
    ConnectorNotFoundError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    StatementDeletedError,
    StatementNotFoundError,
    StatementStoppedError,
    TableflowTopicAlreadyExistsError,
    TableflowTopicNotFoundError,
    TypeMismatchError,
    Warning,
)
from .execution_mode import ExecutionMode
from .result_readers import ChangeloggedRow
from .statement import HIDDEN_LABEL, Op
from .statement_properties import (
    Property,
    PropertyValue,
    ScanStartupMode,
    SnapshotMode,
    SnapshotWriteMode,
    StatementProperties,
)
from .tableflow import (
    AzureAdlsStorage,
    ByobAwsStorage,
    ManagedStorage,
    TableflowErrorHandling,
    TableflowErrorHandlingLog,
    TableflowErrorHandlingSkip,
    TableflowErrorHandlingSuspend,
    TableflowPhase,
    TableflowTopic,
    TableflowTopicConfig,
    TableFormat,
)
from .types import PropertiesDict, PropertiesMapping, SqlNone, YearMonthInterval

# DB-API v2 module globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections
paramstyle = "pyformat"  # Use question mark style parameters

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "ExecutionMode",
    "Op",
    "ChangeloggedRow",
    "ChangelogCompressor",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "ComputePoolExhaustedError",
    "StatementStoppedError",
    "StatementDeletedError",
    "StatementNotFoundError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "TypeMismatchError",
    "TableflowTopicNotFoundError",
    "TableflowTopicAlreadyExistsError",
    "ConnectorNotFoundError",
    "ConnectorAlreadyExistsError",
    "Connector",
    "ConnectorSpec",
    "ConnectorStatus",
    "ConnectorState",
    "TaskStatus",
    "TableFormat",
    "TableflowPhase",
    "TableflowTopic",
    "TableflowTopicConfig",
    "ManagedStorage",
    "ByobAwsStorage",
    "AzureAdlsStorage",
    "TableflowErrorHandling",
    "TableflowErrorHandlingSuspend",
    "TableflowErrorHandlingSkip",
    "TableflowErrorHandlingLog",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "PropertiesDict",
    "PropertiesMapping",
    "Property",
    "PropertyValue",
    "ScanStartupMode",
    "SnapshotMode",
    "SnapshotWriteMode",
    "StatementProperties",
    "SqlNone",
    "YearMonthInterval",
    "HIDDEN_LABEL",
]
