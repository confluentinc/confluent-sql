"""
Confluent SQL - DB-API v2 compliant driver for Confluent SQL.

This module provides a DB-API v2 compliant interface for connecting to and
executing SQL queries against Confluent SQL services.
"""

from .changelog_compressor import ChangelogCompressor
from .connection import Connection, connect
from .cursor import Cursor
from .exceptions import (
    ComputePoolExhaustedError,
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
    StatementStoppedError,
    TypeMismatchError,
    Warning,
)
from .execution_mode import ExecutionMode
from .result_readers import ChangeloggedRow
from .statement import Op
from .types import PropertiesDict, SqlNone, YearMonthInterval

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
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "TypeMismatchError",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "PropertiesDict",
    "SqlNone",
    "YearMonthInterval",
]
