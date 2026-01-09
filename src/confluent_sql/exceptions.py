"""
Exception classes for Confluent SQL DB-API driver.

This module defines the standard DB-API v2 exception hierarchy for the
Confluent SQL driver.
"""

from typing import Any


class Warning(Exception):
    """
    Exception raised for important warnings like data truncations.

    This exception is raised when the database issues a warning that
    should be brought to the user's attention.
    """

    pass


class Error(Exception):
    """
    Exception that is the base class of all other error exceptions.

    This is the base class for all database-related exceptions in the
    DB-API specification.
    """

    pass


class InterfaceError(Error):
    """
    Exception raised for errors related to the database interface.

    This exception is raised when there are problems with the database
    interface rather than the database itself.
    """

    pass


class TypeMismatchError(InterfaceError):
    """Raised when a TypeConverter is being driven with the wrong type, either when
    converting parameter values to SQL literals or when processing Flink statement
    results.

    Subclass of InterfaceError.

    Generally indicates a programming error in the driver."""

    def __init__(self, converter_name: str, method_name: str, expected_type: str, bad_value: Any):
        super().__init__(
            f"Expected {expected_type} value for {converter_name}::{method_name}"
            f" but got {type(bad_value).__name__}"
        )


class DatabaseError(Error):
    """
    Exception raised for errors related to the database.

    This exception is raised when there are problems with the database
    itself, such as connection failures or database-specific errors.
    """

    pass


class DataError(DatabaseError):
    """
    Exception raised for errors due to problems with the processed data.

    This exception is raised when there are problems with the data being
    processed, such as division by zero, numeric value out of range, etc.
    """

    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors related to the database's operation.

    This exception is raised when there are errors that are not under
    the control of the programmer, such as unexpected disconnection,
    the data source name not found, a transaction could not be processed,
    a memory allocation error occurred during processing, etc.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected.

    This exception is raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails, duplicate key, etc.
    """

    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error.

    This exception is raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc.
    """

    pass


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors.

    This exception is raised for programming errors, such as table not
    found or already exists, syntax error in the SQL statement, wrong
    number of parameters specified, etc.
    """

    pass


class NotSupportedError(DatabaseError):
    """
    Exception raised when a method or database API is not supported.

    This exception is raised when a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction
    or has transactions turned off.
    """

    pass
