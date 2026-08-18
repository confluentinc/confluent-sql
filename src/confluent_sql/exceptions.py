"""
Exception classes for Confluent SQL DB-API driver.

This module defines the standard DB-API v2 exception hierarchy for the
Confluent SQL driver.
"""

from __future__ import annotations

from enum import Enum
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


class TableflowTopicNotFoundError(OperationalError):
    """
    Exception raised when a Tableflow topic does not exist for a table's Kafka topic.

    Raised on an HTTP 404 from get_tableflow() or disable_tableflow() -- i.e. Tableflow was
    never enabled (or has already been removed) for the named table. disable_tableflow(
    wait_for_removal=True) polls against this 404 to confirm teardown.

    Attributes:
        table_name: The Flink table / Kafka topic name that had no Tableflow topic.
    """

    def __init__(self, message: str, table_name: str):
        super().__init__(message)
        self.table_name = table_name


class TableflowTopicAlreadyExistsError(OperationalError):
    """
    Exception raised when enabling Tableflow on a topic that already has it enabled.

    Raised on an HTTP 409 from enable_tableflow().

    Attributes:
        table_name: The Flink table / Kafka topic name that already had Tableflow enabled.
    """

    def __init__(self, message: str, table_name: str):
        super().__init__(message)
        self.table_name = table_name


class ConnectorNotFoundError(OperationalError):
    """
    Exception raised when a connector does not exist.

    Raised on an HTTP 404 from get_connector() or delete_connector() -- i.e. the connector was
    never created (or has already been removed). delete_connector(wait_for_removal=True) polls
    against this 404 to confirm teardown.

    Attributes:
        connector_name: The name of the connector that was not found.
    """

    def __init__(self, message: str, connector_name: str):
        super().__init__(message)
        self.connector_name = connector_name


class ConnectorAlreadyExistsError(OperationalError):
    """
    Exception raised when creating a connector whose name is already taken.

    Raised on an HTTP 409 from create_connector().

    Attributes:
        connector_name: The name of the connector that already existed.
    """

    def __init__(self, message: str, connector_name: str):
        super().__init__(message)
        self.connector_name = connector_name


class OAuthLoginFailure(Enum):
    """Why an interactive OAuth browser login could not complete.

    Members are added as later children of the OAuth epic grow new failure modes; the callback
    server (#152) raises the four here.
    """

    TIMED_OUT = "timed_out"
    """The user never completed the browser login within the allotted time."""

    PORT_IN_USE = "port_in_use"
    """The fixed loopback callback port was already bound. Because the port is baked into the
    auth service client's whitelisted redirect_uri, there is no alternate port to fall back to."""

    AUTHORIZATION_DENIED = "authorization_denied"
    """The auth service redirected back with an `error` -- the user declined consent, or the
    authorization request itself was rejected."""

    SERVER_ERROR = "server_error"
    """The local callback listener could not be established, or died after binding."""


class OAuthLoginError(OperationalError):
    """
    Exception raised when an interactive OAuth browser login cannot complete.

    This is a subclass of OperationalError.

    Attributes:
        reason: An `OAuthLoginFailure` naming the cause, for callers that need to react to it
            (e.g. re-prompting on a timeout but not on a denied authorization) rather than
            matching on message text.
    """

    def __init__(self, message: str, reason: OAuthLoginFailure):
        super().__init__(message)
        self.reason = reason


class OAuthTokenEndpointError(OperationalError):
    """
    Exception raised when the authentication service's token endpoint rejects a request.

    Covers both grants this driver sends to `/oauth/token`: the initial `authorization_code`
    exchange and every subsequent `refresh_token` exchange.

    This is a subclass of OperationalError.

    Attributes:
        error_code: The machine-readable `error` field from the token endpoint's OAuth 2.0
            error response (RFC 6749 section 5.2) -- most importantly `"invalid_grant"`, which
            is how the service reports a refresh token that is expired, revoked, or already
            spent. None when the response carried no such field (a non-JSON body, an HTML error
            page from an intermediary, etc.).

    The code is surfaced as an attribute rather than left buried in the message because the
    refresh path has to *act* on it: `invalid_grant` means only a fresh interactive login can
    recover, while a 429 or a 5xx is a blip worth retrying on the next request. Matching that
    distinction on message text would be a guess; matching it on this field is not.
    """

    def __init__(
        self, message: str, error_code: str | None = None, http_status_code: int | None = None
    ):
        super().__init__(message, http_status_code=http_status_code)
        self.error_code = error_code


class ReauthenticationReason(Enum):
    """Why a fresh interactive login is the only way forward.

    Both members mean the same thing operationally -- the refresh token can no longer mint
    tokens -- but they are reached differently, and a caller re-prompting a human benefits from
    knowing which.
    """

    ABSOLUTE_EXPIRY = "absolute_expiry"
    """The refresh token's ~8h absolute lifetime elapsed. Known locally from the token set's own
    expiry, so this is detected *without* spending a doomed request on the token endpoint."""

    REFRESH_REJECTED = "refresh_rejected"
    """The token endpoint refused the refresh token (`invalid_grant`). Covers idle expiry, an
    administrator revoking the session, and a token already spent by someone else -- the service
    reports all three identically, so this driver does not pretend to tell them apart."""


class ReauthenticationRequired(OperationalError):
    """
    Exception raised when an OAuth session can no longer be refreshed and only a fresh
    interactive browser login can recover.

    This is a subclass of OperationalError, raised from the request path once the driver knows
    that no further token refresh can succeed.

    Attributes:
        reason: A `ReauthenticationReason` naming which wall was hit.
    """

    def __init__(self, message: str, reason: ReauthenticationReason):
        super().__init__(message)
        self.reason = reason


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
