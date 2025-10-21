"""
Confluent SQL - DB-API v2 compliant driver for Confluent SQL.

This module provides a DB-API v2 compliant interface for connecting to and
executing SQL queries against Confluent SQL services.
"""

from .connection import connect
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

# DB-API v2 module globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections
paramstyle = "qmark"  # Use question mark style parameters

__all__ = [
    "connect",
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
]
