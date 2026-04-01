"""
Exception classes for Confluent SQL DB-API driver.

This module defines the standard DB-API v2 exception hierarchy for the
Confluent SQL driver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .statement import Phase, Statement


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

    Attributes:
        http_status_code: Optional HTTP status code associated with the error,
            if the error originated from an HTTP API call.
    """

    def __init__(self, message: str, http_status_code: int | None = None):
        super().__init__(message)
        self.http_status_code = http_status_code


class ComputePoolExhaustedError(OperationalError):
    """
    Exception raised when a statement cannot be executed due to compute pool exhaustion.

    This is a subclass of OperationalError.

    Attributes:
        statement_name: The name of the statement that could not be executed.
        statement_deleted: Whether the statement was successfully deleted.
    """

    def __init__(self, message: str, statement_name: str, statement_deleted: bool):
        super().__init__(message)
        self.statement_name = statement_name
        self.statement_deleted = statement_deleted


class StatementStoppedError(OperationalError):
    """
    Exception raised when a streaming statement stops unexpectedly.

    Streaming queries run indefinitely until externally stopped or deleted. When
    the statement enters a terminal phase (STOPPED, FAILED, COMPLETED), this
    exception is raised to indicate the unexpected termination.

    This is a subclass of OperationalError.

    Attributes:
        statement_name: The name of the statement that stopped.
        statement: The Statement object (if available for inspection).
        phase: The terminal phase (STOPPED, FAILED, COMPLETED, etc.) if available.
    """

    def __init__(
        self,
        message: str,
        statement_name: str,
        statement: Statement | None = None,
        phase: Phase | None = None,
    ):
        super().__init__(message)
        self.statement_name = statement_name
        self.statement = statement
        self.phase = phase


class StatementDeletedError(StatementStoppedError):
    """
    Exception raised when attempting to access a statement that has been deleted.

    This is a subclass of StatementStoppedError raised specifically when the server
    returns a 404 status code for a statement that previously existed but has
    since been deleted (either explicitly or by the server).

    Attributes:
        statement_name: The name of the statement that was deleted.
        statement: Always None (deleted statements have no state).
        phase: Always None (deleted statements have no phase).
    """

    def __init__(self, message: str, statement_name: str):
        super().__init__(message, statement_name, statement=None, phase=None)


class StatementNotFoundError(OperationalError):
    """
    Exception raised when attempting to retrieve a statement that does not exist.

    This exception is raised when calling connection.get_statement() with a statement
    name that does not exist in the server, indicated by an HTTP 404 response from
    the GET /statements/{name} endpoint.

    This differs from StatementDeletedError, which is raised when a statement existed
    and results were being fetched, but the statement was deleted while consuming results.

    Attributes:
        statement_name: The name of the statement that was not found.

    Example:
        try:
            stmt = connection.get_statement("non-existent-statement")
        except StatementNotFoundError as e:
            print(f"Statement '{e.statement_name}' not found")
    """

    def __init__(self, message: str, statement_name: str):
        """
        Initialize StatementNotFoundError.

        Args:
            message: Human-readable error message
            statement_name: The name of the statement that was not found
        """
        super().__init__(message)
        self.statement_name = statement_name


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
