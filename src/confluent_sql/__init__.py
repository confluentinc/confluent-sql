"""
Confluent SQL - DB-API v2 compliant driver for Confluent SQL.

This module provides a DB-API v2 compliant interface for connecting to and
executing SQL queries against Confluent SQL services.
"""

from .connection import Connection, connect
from .cursor import Cursor
from .exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TypeMismatchError,
    Warning,
)
from .types import SqlNone, YearMonthInterval, register_row_type

# DB-API v2 module globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections
paramstyle = "pyformat"  # Use question mark style parameters

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "SqlNone",
    "YearMonthInterval",
    "register_row_type",
]
