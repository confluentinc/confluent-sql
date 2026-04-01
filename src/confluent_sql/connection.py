"""
Connection module for Confluent SQL DB-API driver.

This module provides the connect function and Connection class for establishing
connections to Confluent SQL services.
"""

from __future__ import annotations

import logging
import uuid
import warnings
from collections import namedtuple
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from typing import Any

import httpx

from .__version__ import VERSION
from .cursor import Cursor
from .exceptions import (
    InterfaceError,
    OperationalError,
    StatementDeletedError,
    StatementNotFoundError,
)
from .execution_mode import ExecutionMode
from .statement import LABEL_PREFIX as STATEMENT_LABEL_PREFIX
from .statement import ChangelogRow, Statement
from .types import PropertiesDict, RowPythonTypes

logger = logging.getLogger(__name__)


def connect(  # noqa: PLR0913
    *,
    flink_api_key: str,
    flink_api_secret: str,
    environment: str,
    compute_pool_id: str,
    organization_id: str,
    cloud_provider: str | None = None,
    cloud_region: str | None = None,
    database: str | None = None,
    endpoint: str | None = None,
    dbname: str | None = None,  # deprecated, use database parameter
    result_page_fetch_pause_millis: int = 100,
    http_user_agent: str | None = None,
) -> Connection:
    """
    Create a connection to a Confluent SQL service.

    Args:
        flink_api_key: Flink Region API key
        flink_api_secret: Flink Region API secret
        environment: Environment ID
        compute_pool_id: Compute pool ID for SQL execution
        organization_id: Organization ID
        cloud_provider: Cloud provider (e.g., "aws", "gcp", "azure"). Required if endpoint is not
            provided; must not be provided if endpoint is specified.
        cloud_region: Cloud region (e.g., "us-east-2", "us-west-2"). Required if endpoint is not
            provided; must not be provided if endpoint is specified.
        database: The default Flink database (Kafka cluster) to use when resolving
            table/view/udf names (optional)
        endpoint: The base URL for Confluent Cloud API (optional). If not provided, the endpoint
            will be constructed based on the cloud_provider and cloud_region parameters in the
            format "https://flink.{cloud_region}.{cloud_provider}.confluent.cloud". A trailing
            slash is optional and will be stripped if provided (e.g., both
            "https://custom.example.com" and "https://custom.example.com/" are accepted).
        dbname: Deprecated alias for database parameter (optional)
        result_page_fetch_pause_millis: Maximum milliseconds to wait between fetching pages of
            statement results (per statement). Defaults to 100ms. Prevents tight loops of requests
            to the statement results API when consuming results for a statement, especially when
            no results are currently available but more may be forthcoming, such as when
            consuming results from a running streaming query, or prior to when the first page
            of results is ready for a snapshot query.

            If it has already been at least this long since the most recent fetch of results for the
            statement, then no delay will happen.
        http_user_agent: User-Agent header value for HTTP requests. Must be a string
                       between 1-100 characters. Defaults to
                       "Confluent-SQL-Dbapi/v<version> (https://confluent.io; support@confluent.io)"
                       where version is from __version__.py

    Returns:
        A Connection object representing the database connection

    Raises:
        InterfaceError: If connection parameters are invalid
        OperationalError: If connection cannot be established
    """

    if not environment:
        raise InterfaceError("Environment ID is required")

    if not compute_pool_id:
        raise InterfaceError("Compute pool ID is required")

    if not organization_id:
        raise InterfaceError("Organization ID is required")

    if endpoint:
        if cloud_provider or cloud_region:
            raise InterfaceError(
                "cloud_provider and cloud_region should not be provided when endpoint is specified"
            )
    else:
        if not cloud_provider:
            raise InterfaceError("Cloud provider is required when endpoint is not provided")

        if not cloud_region:
            raise InterfaceError("Cloud region is required when endpoint is not provided")

    if not flink_api_key or not flink_api_secret:
        raise InterfaceError("Flink API key and secret are required")

    if dbname is not None:
        warnings.warn(
            "The 'dbname' parameter is deprecated and will be removed in a future release. "
            "Please use the 'database' parameter instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if database is not None:
            raise InterfaceError(
                "Cannot specify both 'database' and deprecated 'dbname' parameters"
            )

    return Connection(
        flink_api_key,
        flink_api_secret,
        environment,
        compute_pool_id,
        organization_id,
        cloud_provider,
        cloud_region,
        endpoint,
        database=database or dbname,  # dbname is deprecated.
        statement_results_page_fetch_pause_millis=result_page_fetch_pause_millis,
        http_user_agent=http_user_agent,
    )


class Connection:
    """
    A connection to a Confluent SQL service.

    This class represents a connection to a Confluent SQL service and provides
    methods for creating cursors and managing the connection lifecycle.
    """

    DEFAULT_USER_AGENT = (
        f"Confluent-SQL-Dbapi/v{VERSION} (https://confluent.io; support@confluent.io)"
    )

    environment: str
    organization_id: str
    compute_pool_id: str
    statement_results_page_fetch_pause_secs: float
    """Maximum seconds to wait between fetching pages of statement
        results (per statement). Prevents tight loops of requests to the
        statement results API when consuming results for a statement, especially when no results are
        currently available but more may be forthcoming, such as when consuming results from
        a running streaming query, or prior to when the first page of results is ready for
        a snapshot query.

        If it has already been at least this long since the most recent fetch of results for the
        statement, then no delay will happen.

        Referenced by the result reader when fetching pages of results for individual
        statements.
    """

    _closed: bool
    _database: str | None
    _client: httpx.Client
    _http_user_agent: str

    _row_type_registry: RowTypeRegistry
    """Registry for user-defined row types, see register_row_type()."""

    _snapshot_warning_issued: bool
    """Internal flag to track whether the snapshot query early access warning has been issued.
        Remove after snapshot queries reach open preview (expected May 2026)."""

    def __init__(  # noqa: PLR0913
        self,
        flink_api_key: str,
        flink_api_secret: str,
        environment: str,
        compute_pool_id: str,
        organization_id: str,
        cloud_provider: str | None,
        cloud_region: str | None,
        endpoint: str | None,
        database: str | None = None,
        statement_results_page_fetch_pause_millis: int = 100,
        http_user_agent: str | None = None,
    ):
        """
        Initialize a new connection to a Confluent SQL service.

        Args:
            flink_api_key: Flink API key
            flink_api_secret: Flink API secret
            environment: Environment ID
            compute_pool_id: Compute pool ID for SQL execution
            organization_id: Organization ID
            cloud_provider: Cloud provider (required if endpoint is not provided)
            cloud_region: Cloud region (e.g., "us-east-2", "us-west-2"). Required if endpoint is
                not provided.
            endpoint: The base URL for Confluent Cloud API (optional). If not provided, the
                endpoint will be constructed based on the cloud_provider and cloud_region
                parameters in the format "https://flink.{cloud_region}.{cloud_provider}.confluent.cloud".
                A trailing slash is optional and will be stripped if provided.
            database: The name of the database to use (optional)
            result_page_fetch_pause_millis: Milliseconds to possibly wait between fetching pages of
                statement results. Defaults to 100ms. If most recent fetch of results for a
                statement was more than this long ago, then no delay will happen when fetching
                the next page of results for the statement.
            http_user_agent: User-Agent header for HTTP requests. String, 1-100 chars.
                           Defaults to the value of DEFAULT_USER_AGENT, which includes the
                           driver name/version, documentation URL, and support email.
        """
        self.environment = environment
        self.compute_pool_id = compute_pool_id
        self.organization_id = organization_id

        if statement_results_page_fetch_pause_millis < 0:
            raise InterfaceError("result_page_fetch_pause_millis must be non-negative")

        # Will be referenced by cursor / result reader when
        # fetching pages of results for individual statements.
        self.statement_results_page_fetch_pause_secs = (
            statement_results_page_fetch_pause_millis / 1000.0
        )

        # Internal state
        self._closed = False
        self._database = database

        # Set user agent (validation happens in setter, default if None)
        self.http_user_agent = (
            http_user_agent if http_user_agent is not None else self.DEFAULT_USER_AGENT
        )

        if not endpoint and not (cloud_provider and cloud_region):
            raise InterfaceError(
                "cloud_provider and cloud_region are required when endpoint is not provided"
            )

        # Create httpx client for making API calls
        if not endpoint:
            # Construct the endpoint URL based on cloud provider and region.
            endpoint = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
        else:
            # Strip trailing slash if user provided one, to ensure clean URL construction
            endpoint = endpoint.rstrip("/")

        base_url = f"{endpoint}/sql/v1/organizations/{organization_id}/environments/{environment}"

        # Create httpx client for making API calls
        basic_auth = httpx.BasicAuth(username=flink_api_key, password=flink_api_secret)
        self._client = httpx.Client(
            auth=basic_auth,
            base_url=base_url,
            headers={
                "Content-Type": "application/json",
                "User-Agent": self._http_user_agent,
            },
        )

        self._row_type_registry = RowTypeRegistry()

        # TODO: remove after snapshot queries reach open preview (May 2026)
        self._snapshot_warning_issued = False

    def close(self) -> None:
        """
        Close the connection.
        """
        if not self._closed:
            self._closed = True
            self._client.close()
        else:
            logger.info("Trying to close a closed connection, ignoring")

    def cursor(self, *, as_dict: bool = False, mode=ExecutionMode.SNAPSHOT) -> Cursor:
        """
        Create a new cursor for executing statements. Defaults to creating
        a snapshot (bounded) query cursor for returning point-in-time results.

        Snapshot queries will return results from a consistent point in time, and
        the result stream is considered both bounded and append-only, and will
        only be generated when the query execution has completed, having consumed
        all source data as of the query start time.

        Streaming queries will return results as they are produced by the executing query,
        but may or may not be append-only depending on the query characteristics. For example,
        a streaming query that only filters from source tables (Kafka topics) will be append-only,
        but a streaming query that performs aggregations or joins will not be, as updates to
        previously emitted results may occur as more data is processed. Non-append-only streaming
        query results will include a changelog operation with each row indicating whether the row
        is an insertion, update, or deletion, indicated by the 'op' field in the returned
        ChangeloggedRow namedtuple.

        So, while mode=ExecutionMode.STREAMING_QUERY will always initiate a streaming query,
        the presence of changelog operations in the results depends on whether the
        query submitted will result in append-only processing or not.

        See the documentation in the Cursor class for more details on the behavior
        of the cursor, its fetch method and iteration behavior, as to the differences
        between snapshot and streaming queries.

        The cursor's fetch methods return different types based on configuration
        and query characteristics:

        Return Type Matrix
        ------------------
        1. **Append-only queries + as_dict=False** (default):
           Returns tuples: `("val1", "val2", ...)`
           Standard DB-API format for regular SELECT queries

        2. **Append-only queries + as_dict=True**:
           Returns dicts: `{"col1": "val1", "col2": "val2"}`
           Column names as keys for better readability

        3. **Changelog queries + as_dict=False** (streaming non-append-only, row as tuples):
           Returns ChangeloggedRow namedtuples: `ChangeloggedRow(op=Op.INSERT,row=("v1", "v2"))`
           Includes operation type (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE) with row data

        4. **Changelog queries + as_dict=True** (streaming non-append-only, row as dicts):
           Returns ChangeloggedRow with dict: `ChangeloggedRow(op=Op.INSERT, row={"col1": "val1"})`
           Combines operation tracking with named column access

        Args:
            as_dict: If True, return row data as dictionaries with column names as keys.
                     If False (default), return row data as tuples.
            mode: The execution mode for the cursor. Defaults to SNAPSHOT for bounded
                  queries. Use STREAMING_QUERY for continuous/unbounded queries.

        Returns:
            A new Cursor object associated with this connection

        Raises:
            InterfaceError: If the connection is closed

        Examples:
            # Standard snapshot query with tuples
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users")
            assert not cursor.is_streaming
            row = cursor.fetchone()  # Returns: ("Alice", 25), or None if no more rows, period.

            # Snapshot query with dicts
            cursor = conn.cursor(as_dict=True)
            cursor.execute("SELECT * FROM users")
            assert cursor.as_dict == True
            assert not cursor.is_streaming
            row = cursor.fetchone() # Returns: {"name": "Alice", "age": 25} or None if no more rows

            # Streaming append-only query with tuples
            cursor = conn.cursor(mode=ExecutionMode.STREAMING_QUERY)
            assert cursor.is_streaming
            cursor.execute("SELECT user_id FROM orders")
            assert not cursor.returns_changelog # Will not be known until after execute().
            while cursor.may_have_results:
                # Returns either ("Alice",) or None if _no data available at this time_.
                row = cursor.fetchone()
                if row is not None:
                    ...

            # Streaming changelog query
            cursor = conn.cursor(mode=ExecutionMode.STREAMING_QUERY)
            cursor.execute("SELECT user_id, count(*) from orders group by user_id")
            assert cursor.is_streaming
            assert cursor.returns_changelog
            while cursor.may_have_results:
                row = cursor.fetchone()
                # may return None if _no data available at this time_
                if row is not None:
                    # Returns a ChangeloggedRow namedtuple:
                    # ChangeloggedRow(op=Op.INSERT, row=("Alice", 25))


        """
        if self._closed:
            raise InterfaceError("Connection is closed")

        # TODO: remove after snapshot queries reach open preview (May 2026)
        if mode.is_snapshot and not self._snapshot_warning_issued:
            self._snapshot_warning_issued = True
            warnings.warn(
                "Snapshot queries on Confluent Cloud Flink SQL are currently in "
                "Early Access and may be subject to change.",
                stacklevel=2,
            )

        return Cursor(self, as_dict=as_dict, execution_mode=mode)

    def streaming_cursor(self, *, as_dict: bool = False) -> Cursor:
        """
        Create a streaming query cursor. Waits for RUNNING, iterates over continuous results.

        This is a convenience method equivalent to:
        `cursor(as_dict=as_dict, mode=ExecutionMode.STREAMING_QUERY)`

        For streaming queries, the return type depends on whether the query is append-only:
        - Append-only: Returns tuples or dicts based on as_dict parameter
        - Non-append-only: Returns ChangeloggedRow namedtuples containing operation and row data

        See cursor() method documentation for detailed return type information.

        Args:
            as_dict: If True, return row data as dictionaries. If False, as tuples.

        Returns:
            A new Cursor configured for streaming query execution
        """
        return Cursor(self, as_dict=as_dict, execution_mode=ExecutionMode.STREAMING_QUERY)

    @contextmanager
    def closing_cursor(
        self, *, as_dict: bool = False, mode: ExecutionMode = ExecutionMode.SNAPSHOT
    ) -> Generator[Cursor, None, None]:
        """
        Context manager for creating and automatically closing a cursor.

        Creates a cursor with the same return type variations as cursor() method.
        See cursor() documentation for details on the four possible return types
        based on as_dict and query characteristics (append-only vs changelog).

        Args:
            as_dict: If True, fetch results as dictionaries, otherwise as tuples
            mode: The execution mode for the cursor. Defaults to SNAPSHOT.

        Yields:
            A new Cursor object associated with this connection

        Raises:
            InterfaceError: If the connection is closed

        Example:
            with conn.closing_cursor(as_dict=True) as cursor:
                cursor.execute("SELECT * FROM users")
                for row in cursor:
                    print(row)  # Prints dicts with column names
            # cursor is automatically closed after the with block
        """
        cursor = self.cursor(as_dict=as_dict, mode=mode)
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def closing_streaming_cursor(self, *, as_dict: bool = False) -> Generator[Cursor, None, None]:
        """
        Context manager for creating and automatically closing a streaming cursor.

        Convenience method equivalent to:
            closing_cursor(as_dict=as_dict, mode=ExecutionMode.STREAMING_QUERY)

        Creates a streaming cursor that processes continuous data from Flink SQL
        with automatic cleanup. Streaming cursors return data as it arrives without
        blocking or collecting all results into memory.

        Statement Lifecycle Management:
            The context manager automatically closes the cursor via cursor.close(),
            which makes a best-effort attempt to delete statements that are already
            in terminal phases (COMPLETED/FAILED/STOPPED). Deletion errors are
            logged and suppressed, so server-side cleanup is not strictly
            guaranteed. Long-running streaming queries that remain RUNNING on the
            server after exiting the context manager are NOT automatically stopped
            or deleted server-side.

            To explicitly stop a RUNNING streaming statement, call
            cursor.delete_statement() or connection.delete_statement(statement_id)
            before exiting the context manager.

        Args:
            as_dict: If True, fetch results as dictionaries, otherwise as tuples

        Yields:
            A new streaming Cursor object associated with this connection

        Raises:
            InterfaceError: If the connection is closed

        Example:
            with conn.closing_streaming_cursor(as_dict=True) as cursor:
                cursor.execute("SELECT * FROM orders WHERE amount > %s", (1000,))
                while cursor.may_have_results:
                    rows = cursor.fetchmany(10)
                    if rows:
                        for row in rows:
                            process(row)
                    else:
                        time.sleep(0.1)
            # cursor is automatically closed after the with block
        """
        with self.closing_cursor(as_dict=as_dict, mode=ExecutionMode.STREAMING_QUERY) as cursor:
            yield cursor

    def execute_snapshot_ddl(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        timeout: int = 3000,
        statement_name: str | None = None,
        statement_labels: list[str] | None = None,
        properties: PropertiesDict | None = None,
    ) -> Statement:
        """Execute bounded DDL that completes after consuming snapshot data.

        Use for statements like:
        - CREATE TABLE (not AS SELECT)
        - DROP TABLE
        - ALTER TABLE
        - CREATE VIEW
        - DROP VIEW
        - CREATE TABLE foo AS SELECT ... (snapshot mode, where the SELECT portion completes
                                            with snapshot behavior)


        Args:
            statement_text: The DDL statement to execute
            parameters: Optional statement parameters
            timeout: Maximum time to wait for completion in seconds
            statement_name: Optional name for the statement
            statement_labels: Optional list of labels for the statement. Labels can be used to
                             group and manage related statements. Each label will be
                             prefixed with "user.confluent.io/" when stored but you only
                             need to provide the label values (e.g., ["my-ddl-batch", "daily"]).
                             Use HIDDEN_LABEL to mark statements as hidden from default views.
            properties: Optional dictionary of statement properties to set for this execution.
                       Keys must be strings, values must be str, int, or bool.

        Returns:
            Statement for managing the statement lifecycle

        Raises:
            OperationalError: If statement fails or times out
            ProgrammingError: If statement is invalid
        """
        with self.closing_cursor(mode=ExecutionMode.SNAPSHOT_DDL) as cur:
            cur.execute(
                statement_text,
                parameters,
                timeout=timeout,
                statement_name=statement_name,
                statement_labels=statement_labels,
                properties=properties,
            )

        # Return the last version of the statement
        return cur.statement

    def execute_streaming_ddl(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        timeout: int = 3000,
        statement_name: str | None = None,
        statement_labels: list[str] | None = None,
        properties: PropertiesDict | None = None,
    ) -> Statement:
        """Execute unbounded DDL that starts a streaming job.

        Use for statements like:
        - CREATE TABLE ... AS SELECT ... (streaming mode, where the SELECT portion is unbounded)
        - CREATE MATERIALIZED TABLE ... (streaming mode, where the table is populated by an
                                            unbounded streaming job but the overall CREATE statement
                                            itself completes once the population job is started)

        Args:
            statement_text: The DDL statement to execute
            parameters: Optional statement parameters
            timeout: Maximum time to wait for completion in seconds
            statement_name: Optional name for the statement
            statement_labels: Optional list of labels for the statement. Labels can be used to
                             group and manage related statements. Each label will be
                             prefixed with "user.confluent.io/" when stored but you only
                             need to provide the label values (e.g., ["streaming-jobs", "daily"]).
                             Use HIDDEN_LABEL to mark statements as hidden from default views.
            properties: Optional dictionary of statement properties to set for this execution.
                       Keys must be strings, values must be str, int, or bool.
        Returns:
            Statement for any further management of the statement lifecycle
        """

        with self.closing_cursor(mode=ExecutionMode.STREAMING_DDL) as cur:
            cur.execute(
                statement_text,
                parameters,
                timeout=timeout,
                statement_name=statement_name,
                statement_labels=statement_labels,
                properties=properties,
            )

        return cur.statement

    def list_statements(self, *, label: str, page_size: int = 100) -> list[Statement]:
        """Return a list of Statement objects for statements with the given label.

        This method retrieves all statements that were created with the specified label,
        which is useful for managing groups of related statements. The method handles
        pagination automatically to retrieve all matching statements.

        Args:
            label: The label to filter statements by. You can provide either:
                   - Just the label value (e.g., "my-batch-job") - the "user.confluent.io/"
                     prefix will be added automatically
                   - The full label with prefix (e.g., "user.confluent.io/my-batch-job")
            page_size: Number of statements to fetch per API request (default: 100).
                      The method will automatically paginate through all results.

        Returns:
            A list of Statement objects that have the specified label. Returns an
            empty list if no statements match the label.

        Raises:
            OperationalError: If the API request fails

        Example:
            # Submit statements with a label
            cursor.execute("SELECT * FROM users", statement_labels=["daily-report"])
            cursor.execute("SELECT * FROM orders", statement_labels=["daily-report"])

            # Later, retrieve all statements with that label
            statements = connection.list_statements(label="daily-report")

            # Delete all statements with the label
            for stmt in statements:
                connection.delete_statement(stmt)
        """

        if not label.startswith(STATEMENT_LABEL_PREFIX):
            # Append prefix and make it a label selector for the API query parameter. The API
            # expects the full label key, which includes the prefix, but we want to allow users
            # to filter by just the end-user portion of the label.
            adjusted_label_filter = f"{STATEMENT_LABEL_PREFIX}{label}=true"
        else:
            adjusted_label_filter = f"{label}=true"

        statements: list[Statement] = []

        has_more_pages = True
        next_page_token: str | None = None
        # Use the `label_selector` query parameter to filter statements by label
        # on the server side.
        parameters = {"label_selector": adjusted_label_filter, "page_size": page_size}
        while has_more_pages:
            response = self._request("/statements", params=parameters)
            resp_json = response.json()
            statements_json = resp_json.get("data", [])
            statements.extend(Statement.from_response(self, s) for s in statements_json)

            # Check if there are more pages to fetch based on the presence of a 'next' link in the
            # response metadata. The 'next' value will be an entire URL, but we just need to extract
            # the page token from it for the next request.
            next_page_token = self._get_next_page_token(resp_json.get("metadata", {}).get("next"))
            if next_page_token:
                parameters["page_token"] = next_page_token
            has_more_pages = next_page_token is not None

        return statements

    def get_statement(self, statement: str | Statement) -> Statement:
        """
        Get the current state of a statement by name or Statement object.

        Retrieves the latest status and metadata for a statement from the server,
        including phase (PENDING, RUNNING, COMPLETED, FAILED, etc.), schema,
        and execution traits. Useful for polling statement progress or checking
        if results are ready to fetch.

        Args:
            statement: The name of the statement to retrieve, or a Statement object.
                      If a Statement object is passed, its current state will be
                      refreshed from the server.

        Returns:
            A Statement object representing the current state of the statement.
            If a Statement object was passed as input, a new Statement object
            with updated metadata and status from the server will be returned.

        Raises:
            TypeError: If statement is neither a string nor a Statement object
            StatementNotFoundError: If statement does not exist (HTTP 404)
            OperationalError: If other API errors occur

        Example:
            # Get statement by name
            stmt = connection.get_statement("my-statement-name")
            print(f"Status: {stmt.phase}")  # e.g., "RUNNING" or "COMPLETED"

            # Refresh a Statement object's state
            stmt = connection.get_statement(stmt)
            if stmt.can_fetch_results(ExecutionMode.SNAPSHOT):
                # Results are ready
                ...
        """
        if isinstance(statement, Statement):
            statement_name = statement.name
        else:
            if not isinstance(statement, str):
                raise TypeError(
                    "Statement must be specified by name or Statement object, "
                    f"got {type(statement)}"
                )
            statement_name = statement

        logger.info(f"Getting statement '{statement_name}'")
        response_dict = self._get_statement(statement_name)
        return Statement.from_response(self, response_dict)

    def delete_statement(self, statement: str | Statement) -> None:
        """
        Delete a statement by name or Statement object.

        In Flink SQL, executed statements (especially streaming ones) create
        resources that linger on within CCLoud until explicitly deleted (or
        have stopped and enough time has passed for automatic cleanup).

        Deleting a RUNNING statement will stop it first.

        Args:
            statement: The name of the statement to delete, or the Statement object. If passed
            a Statement object that is already deleted, the deletion is ignored. However, if
            passed a Statement object representing a still running statement, the delete
            operation will be performed, causing the statement to be stopped and deleted.
        """

        if isinstance(statement, Statement):
            if statement.is_deleted:
                logger.info(f"Statement {statement.name} is already deleted, ignoring")
                return
            statement_name = statement.name
        else:
            if not isinstance(statement, str):
                raise TypeError(
                    "Statement to delete must be specified by name or Statement object, "
                    f"got {type(statement)}"
                )

            statement_name = statement

        logger.info(f"Deleting statement {statement_name}")
        response = self._request(
            f"/statements/{statement_name}", method="DELETE", raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                raise OperationalError("Error deleting statement") from e
            # If the response is 404, it means we don't need to delete the statement.
            logger.info(f"Statement '{statement_name}' not found while deleting, ignoring")

        if isinstance(statement, Statement):
            # Mark the Statement object as deleted for if the caller still is interested in its
            # reference.
            statement.set_deleted()

    @property
    def is_closed(self) -> bool:
        """
        Check if the connection is closed.

        Returns:
            True if the connection is closed, False otherwise
        """
        return self._closed

    @property
    def http_user_agent(self) -> str:
        """
        Get the User-Agent header value sent with all HTTP requests.

        Returns:
            The current User-Agent string
        """
        return self._http_user_agent

    @http_user_agent.setter
    def http_user_agent(self, value: str) -> None:
        """
        Set the User-Agent header value for all HTTP requests made by this connection.

        The User-Agent identifies the client software making requests to Confluent Cloud.
        This is useful for tracking, debugging, and analytics purposes.

        Args:
            value: The User-Agent string to use. Must be a non-empty string between
                   1 and 100 characters in length.

        Raises:
            InterfaceError: If value is not a string, is empty, or exceeds 100 characters

        Example:
            conn.http_user_agent = "my-app/1.0"
        """
        if not isinstance(value, str):
            raise InterfaceError(f"http_user_agent must be a string, got {type(value).__name__}")

        if len(value) < 1 or len(value) > 100:
            raise InterfaceError(
                f"http_user_agent length must be between 1 and 100 characters, got {len(value)}"
            )

        self._http_user_agent = value

        # Update the httpx client headers if client is already initialized
        if hasattr(self, "_client"):
            self._client.headers["User-Agent"] = value

    def register_row_type(self, class_for_flink_row: type[RowPythonTypes]) -> None:
        """Register a user-defined namedtuple, NamedTuple, or @dataclass class to be used
        to return deserialized ROW values.

        The user-provided class to use when deserializing a ROW in any particular resultset is
        determined by matching the sequence of ROW field names to the ordered sequence of declared
        field names in the user-provided namedtuple, NamedTuple or @dataclass class.

        If no user-registered class matches the field names of a ROW type in a resultset,
        a new namedtuple class will be created and cached for future use.
        """

        self._row_type_registry.register_row_type(class_for_flink_row)

    _NEVER_USER_PROVIDED_PROPERTIES = {
        "sql.current-catalog",
        "sql.current-database",
        "sql.snapshot.mode",
    }

    def _resolve_properties(
        self, properties: PropertiesDict | None, execution_mode: ExecutionMode
    ) -> PropertiesDict:
        """
        Validate and merge user properties with system properties.

        Validates the properties parameter and merges it with system-level properties
        (catalog, database, snapshot mode). System properties always have precedence
        and cannot be overridden by user input.

        Args:
            properties: Optional dictionary of user-provided statement properties.
                       Keys must be strings, values must be str, int, or bool.
            execution_mode: The execution mode (determines if snapshot.mode is set).

        Returns:
            Merged properties dictionary with system properties overlaid.

        Raises:
            InterfaceError: If properties parameter is invalid (not a dict, invalid keys/values).
        """
        # Validate properties parameter if provided
        if properties is not None:
            if not isinstance(properties, dict):
                raise InterfaceError(f"properties must be a dict, got {type(properties).__name__}")

            for key, value in properties.items():
                if not isinstance(key, str):
                    raise InterfaceError(
                        f"properties keys must be strings, got {type(key).__name__} for key {key!r}"
                    )
                if not isinstance(value, (str, int, bool)):
                    raise InterfaceError(
                        f"properties values must be str, int, or bool, "
                        f"got {type(value).__name__} for key {key!r}"
                    )
                if key in self._NEVER_USER_PROVIDED_PROPERTIES:
                    raise InterfaceError(f"'{key}' is a reserved system property.")

        # Start with user properties (if provided), then overlay system properties
        # This ensures system properties always win and cannot be overridden
        merged_properties: PropertiesDict = {}

        if properties is not None:
            # User properties applied first
            merged_properties.update(properties)

        # Connection-level properties overlay (always set, cannot be overridden by user)
        merged_properties["sql.current-catalog"] = self.environment

        if self._database is not None:
            merged_properties["sql.current-database"] = self._database

        # Cursor-level execution mode properties overlay (always set when applicable)
        if execution_mode.is_snapshot:
            # Ask for snapshot mode behavior -- point-in-time results.
            merged_properties["sql.snapshot.mode"] = "now"

        return merged_properties

    def _execute_statement(
        self,
        statement: str,
        execution_mode: ExecutionMode,
        statement_name: str | None = None,
        statement_labels: list[str] | None = None,
        properties: PropertiesDict | None = None,
    ) -> dict[str, Any]:
        """
        Execute a SQL statement and return the response. Any statement parameters must have already
        been incorproated into the statement string, in that the create statement endpoint
        does not support separate parameter passing.

        Args:
            statement: The SQL statement to execute
            execution_mode: The execution mode for the statement (snapshot, streaming query, etc.)
            statement_name: Optional name for the statement (defaults to 'dbapi-{uuid}')
            statement_labels: Optional list of labels for the statement for easier identification
                             in server logs and UIs (defaults to None). Use HIDDEN_LABEL to mark
                             statements as hidden from default views.
            properties: Optional dictionary of statement properties to set for this execution.
                       Keys must be strings, values must be str, int, or bool. System
                       properties (sql.current-catalog, sql.current-database, sql.snapshot.mode)
                       are always set by the driver and cannot be overridden.

        Returns:
            Dictionary containing the API response

        Raises:
            OperationalError: If statement execution fails
            InterfaceError: If properties parameter is invalid or if statement_labels is invalid
        """

        # Create the statement payload as per Flink SQL API documentation
        if statement_name is None:
            statement_name = f"dbapi-{str(uuid.uuid4())}"

        # Resolve and merge user properties with system properties
        merged_properties = self._resolve_properties(properties, execution_mode)

        payload = {
            "name": statement_name,
            "organization_id": self.organization_id,
            "environment_id": self.environment,
            "spec": {
                "statement": statement,
                "properties": merged_properties,
                "compute_pool_id": self.compute_pool_id,
                "stopped": False,
            },
        }

        if statement_labels is not None:
            if not isinstance(statement_labels, list):
                raise InterfaceError(
                    f"statement_labels must be a list of strings, "
                    f"got {type(statement_labels).__name__}"
                )

            # Treat empty list as equivalent to None - no labels to add
            if len(statement_labels) > 0:
                labels_dict = {}
                for label in statement_labels:
                    if not isinstance(label, str):
                        raise InterfaceError(
                            f"All statement labels must be strings, got {type(label).__name__}"
                        )

                    # Guard against user already including the mandatory prefix
                    if label.startswith(STATEMENT_LABEL_PREFIX):
                        label_key = label
                    else:
                        label_key = f"{STATEMENT_LABEL_PREFIX}{label}"

                    labels_dict[label_key] = "true"

                payload["metadata"] = {"labels": labels_dict}

        # Submit statement using the API
        res = self._request("/statements", method="POST", json=payload)
        return res.json()

    def _get_statement(self, statement_name: str) -> dict[str, Any]:
        """
        INTERNAL USE ONLY. Get raw statement API response.

        For public use, call get_statement() which returns a Statement object.
        This internal method returns the raw API JSON response.

        Args:
            statement_name: The name of the statement to check

        Returns:
            Dictionary containing the statement status and details

        Raises:
            StatementNotFoundError: If statement does not exist (HTTP 404)
            OperationalError: If other API errors occur
        """
        try:
            return self._request(f"/statements/{statement_name}").json()
        except OperationalError as e:
            # Check if this is a 404 error
            if "404" in str(e):
                raise StatementNotFoundError(
                    f"Statement '{statement_name}' not found",
                    statement_name=statement_name,
                ) from e
            # Re-raise other operational errors
            raise

    def _get_statement_results(
        self, statement_name: str, next_url: str | None
    ) -> tuple[list[ChangelogRow], str | None]:
        """
        Try to get a page of results for a statement.

        Args:
            statement_name: The name of the statement
            next_url: Optional full URL to fetch the next page of results from. If None, then
                        the results endpoint for the statement will be used.

        Returns:
            A 2-tuple: (list of results in changelog row format, optional url to fetch next page.)
            If the next page URL is None, there are no more pages to fetch.

        Raises:
            StatementDeletedError: If the statement has been deleted (404)
            OperationalError: If results retrieval fails for other reasons
        """
        if next_url is None:
            next_url = f"/statements/{statement_name}/results"

        try:
            response = self._request(next_url).json()
        except OperationalError as e:
            # Check if this is a 404 error indicating the statement was deleted
            if "404" in str(e):
                raise StatementDeletedError(
                    f"Statement '{statement_name}' has been deleted", statement_name
                ) from e
            raise

        # Check if the response indicates an error (e.g., statement not found)
        # Some APIs return 200 OK with an error payload instead of proper HTTP status codes
        if response is None:
            raise StatementDeletedError(
                f"Statement '{statement_name}' has been deleted", statement_name
            )

        # Promote from the pure from-response-json 'data' sub-member list of dicts
        # to a list of ChangelogRow.
        data_list = response.get("results", {}).get("data")
        if data_list is None:
            # Check if this is an error response indicating the statement was deleted
            error = response.get("error")
            if error:
                error_code = error.get("code")
                if error_code == 404 or "not found" in str(error).lower():
                    raise StatementDeletedError(
                        f"Statement '{statement_name}' has been deleted", statement_name
                    )
                raise OperationalError(f"Error fetching results: {error}")
            # If no error but data is None, treat as deleted statement
            raise StatementDeletedError(
                f"Statement '{statement_name}' has been deleted or returned invalid response",
                statement_name,
            )

        # Promote to ChangelogRow namedtuples, which include the 'op' field for changelog queries,
        # defaulting to 0 (INSERT) if not present. If no (new) results are currently available, this
        # will be an empty list.
        results: list[ChangelogRow] = [
            # 'op' may be omitted, in which case we assume 0 (INSERT)
            ChangelogRow(r.get("op", 0), r["row"])
            for r in data_list
        ]

        logger.info(f"got {len(results)} changelog rows for statement {statement_name}")
        next_url = response.get("metadata", {}).get("next") or None

        return (results, next_url)

    def _request(self, url, method="GET", raise_for_status=True, **kwargs) -> httpx.Response:
        if self._closed:
            raise InterfaceError("Connection is closed")

        try:
            response = self._client.request(method, url, **kwargs)
            logger.debug("Response: %s", response.content)
            if raise_for_status:
                response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            try:
                res = e.response.json()
                errors = res.get("errors", [])
                details = "; ".join([err["detail"] for err in errors])
            except Exception:
                details = "no more details"

            raise OperationalError(
                f"error sending request '{e.response.status_code}' - {details}"
            ) from e

    def _get_next_page_token(self, next_url: str | None) -> str | None:
        """Extract the next page token from the next_url, if present."""
        if next_url is None:
            return None

        # The next_url is expected to be a full URL with a query parameter like '?page_token=abc123'
        # We can parse it to extract the page_token value.
        parsed = httpx.URL(next_url)
        page_token = parsed.params.get("page_token")
        return page_token


class RowTypeRegistry:
    """Registry for namedtuple, NamedTuple or @dataclass classes used for deserializing
    ROW values from query results.

    Users can register their own classes to be used for specific
    field structures via `connection.register_row_type()`. Then any query results
    returning ROW values with matching field names will be deserialized
    into instances of the user-registered class.

    Otherwise, if no user-registered class matches the field names, a new
    namedtuple class will be created and cached for future use.
    """

    _cache: dict[tuple[str, ...], type[RowPythonTypes]]

    def __init__(self):
        # Key: tuple of field names (strings)
        # Value: The specific class object (type)
        self._cache = {}

    def get_row_class(self, field_names: list[str] | tuple[str, ...]) -> type[RowPythonTypes]:
        """
        Returns the cached user-provided class for handling ROWs with the given field names.
        If none found, creates a namedtuple class (and caches it).

        field_names: A sequence of strings (e.g., ['name', 'age'])
        """

        if not isinstance(field_names, (list, tuple)):
            raise TypeError(
                f"field_names must be a list or tuple of strings, got {type(field_names)}"
            )

        for field in field_names:
            if not isinstance(field, str):
                raise TypeError(f"All field names must be strings, got a {type(field)}")

        # Create a hashable key from the field names
        key = tuple(field_names)

        if key not in self._cache:
            # Create a default class name, e.g., 'Row'
            # rename=True handles Flink columns with chars invalid in Python
            new_class = namedtuple("Row", field_names, rename=True)
            logger.debug(
                f"Created new namedtuple class for ROW with fields: {field_names}, "
                f"resulting namedtuple fields: {new_class._fields}"
            )  # pyright: ignore[reportAttributeAccessIssue]
            self._cache[key] = new_class

        return self._cache[key]

    def register_row_type(self, user_type_for_row: type[RowPythonTypes]) -> None:
        """
        Registers a user-provided namedtuple, typing.NamedTuple, or @dataclass class by
        the sequence of its field names for future use when deserializing ROW values.

        Raises TypeError if the provided type is not a supported class type.
        """

        key: tuple[str, ...] | None = None

        if isinstance(user_type_for_row, type):
            # Check for duck-typed namedtuple or typing.NamedTuple: subclass of tuple + has _fields
            if issubclass(user_type_for_row, tuple) and hasattr(user_type_for_row, "_fields"):
                key = tuple(user_type_for_row._fields)  # pyright: ignore[reportAttributeAccessIssue]

            # Only other supported type is an @dataclass
            elif is_dataclass(user_type_for_row):
                key = tuple(field.name for field in fields(user_type_for_row))

        if key is None:
            # User passed a non-supported type or an instance of something.
            raise TypeError(
                f"Expected a namedtuple, NamedTuple, or @dataclass type, got {user_type_for_row} instead"  # noqa: E501
            )

        # Update the cache to prefer the user's class for this structure
        self._cache[key] = user_type_for_row
