"""
Connection module for Confluent SQL DB-API driver.

This module provides the connect function and Connection class for establishing
connections to Confluent SQL services.
"""

from __future__ import annotations

import logging
import uuid
from collections import namedtuple
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from typing import Any

import httpx

from .cursor import Cursor
from .exceptions import InterfaceError, OperationalError
from .execution_mode import ExecutionMode
from .statement import ChangelogRow, Statement
from .types import RowPythonTypes

logger = logging.getLogger(__name__)


def connect(  # noqa: PLR0913
    flink_api_key: str,
    flink_api_secret: str,
    environment: str,
    compute_pool_id: str,
    organization_id: str,
    cloud_provider: str,
    cloud_region: str,
    api_key: str | None = None,
    api_secret: str | None = None,
    dbname: str | None = None,
    result_page_fetch_pause_millis: int = 100,
) -> Connection:
    """
    Create a connection to a Confluent SQL service.

    Args:
        flink_api_key: Flink API key
        flink_api_secret: Flink API secret
        environment: Environment ID
        compute_pool_id: Compute pool ID for SQL execution
        organization_id: Organization ID
        cloud_provider: Cloud provider (e.g., "aws", "gcp", "azure")
        cloud_region: Cloud region (e.g., "us-east-2", "us-west-2")
        api_key: Confluent Cloud API key (optional, for general Confluent Cloud resources)
        api_secret: Confluent Cloud API secret (optional)
        dbname: The name of the database to use (optional)
        result_page_fetch_pause_millis: Maximum milliseconds to wait between fetching pages of
            statement results (per statement). Defaults to 100ms. Prevents tight loops of requests
            to the statement results API when consuming results for a statement, especially when
            no results are currently available but more may be forthcoming, such as when
            consuming results from a running streaming query, or prior to when the first page
            of results is ready for a snapshot query.

            If it has already been at least this long since the most recent fetch of results for the
            statement, then no delay will happen.

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

    if not cloud_provider:
        raise InterfaceError("Cloud provider is required")

    if not cloud_region:
        raise InterfaceError("Cloud region is required")

    if not flink_api_key or not flink_api_secret:
        raise InterfaceError("Flink API key and secret are required")

    return Connection(
        flink_api_key,
        flink_api_secret,
        environment,
        compute_pool_id,
        organization_id,
        cloud_provider,
        cloud_region,
        api_key=api_key,
        api_secret=api_secret,
        dbname=dbname,
        statement_results_page_fetch_pause_millis=result_page_fetch_pause_millis,
    )


class Connection:
    """
    A connection to a Confluent SQL service.

    This class represents a connection to a Confluent SQL service and provides
    methods for creating cursors and managing the connection lifecycle.
    """

    environment: str
    organization_id: str
    compute_pool_id: str
    api_key: str | None
    api_secret: str | None
    host: str | None
    statement_results_page_fetch_pause_secs: float
    """Maximum seconds to wait between fetching pages of statement
        results (per statement). Prevents tight loops of requests to the
        statement results API when consuming results for a statement, especially when no results are
        currently available but more may be forthcoming, such as when consuming results from
        a running streaming query, or prior to when the first page of results is ready for
        a snapshot query.

        If it has already been at least this long since the most recent fetch of results for the
        statement, then no delay will happen.

        Referenced by the changelog processor when fetching pages of results for individual
        statements.
    """

    _closed: bool
    _dbname: str | None
    _client: httpx.Client

    _row_type_registry: RowTypeRegistry
    """Registry for user-defined row types, see register_row_type()."""

    def __init__(  # noqa: PLR0913
        self,
        flink_api_key: str,
        flink_api_secret: str,
        environment: str,
        compute_pool_id: str,
        organization_id: str,
        cloud_provider: str,
        cloud_region: str,
        api_key: str | None = None,
        api_secret: str | None = None,
        host: str | None = None,
        dbname: str | None = None,
        statement_results_page_fetch_pause_millis: int = 100,
    ):
        """
        Initialize a new connection to a Confluent SQL service.

        Args:
            flink_api_key: Flink API key
            flink_api_secret: Flink API secret
            environment: Environment ID
            compute_pool_id: Compute pool ID for SQL execution
            organization_id: Organization ID
            cloud_provider: Cloud provider
            cloud_region: Cloud region (e.g., "us-east-2", "us-west-2")
            result_page_fetch_pause_millis: Milliseconds to possibly wait between fetching pages of
                statement results. Defaults to 100ms. If most recent fetch of results for a
                statement was more than this long ago, then no delay will happen when fetching
                the next page of results for the statement.
            api_key: Confluent Cloud API key for general Confluent Cloud resources (optional)
            api_secret: Confluent Cloud API secret for general Confluent Cloud resources (optional)
            host: The base URL for Confluent Cloud API (optional)
            dbname: The name of the database to use (optional)
        """
        self.environment = environment
        self.compute_pool_id = compute_pool_id
        self.organization_id = organization_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.host = host

        if statement_results_page_fetch_pause_millis < 0:
            raise InterfaceError("result_page_fetch_pause_millis must be non-negative")

        # Will be referenced by cursor / changelog processor when
        # fetching pages of results for individual statements.
        self.statement_results_page_fetch_pause_secs = (
            statement_results_page_fetch_pause_millis / 1000.0
        )

        # Internal state
        self._closed = False
        self._dbname = dbname

        # Create httpx client for making API calls
        if self.host is None:
            self.host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
        base_url = f"{self.host}/sql/v1/organizations/{organization_id}/environments/{environment}"

        # Create httpx client for making API calls
        basic_auth = httpx.BasicAuth(username=flink_api_key, password=flink_api_secret)
        self._client = httpx.Client(
            auth=basic_auth,
            base_url=base_url,
            headers={"Content-Type": "application/json"},
        )

        self._row_type_registry = RowTypeRegistry()

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

    def execute_snapshot_ddl(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        timeout: int = 3000,
        statement_name: str | None = None,
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

        Returns:
            Statement for managing the statement lifecycle

        Raises:
            OperationalError: If statement fails or times out
            ProgrammingError: If statement is invalid
        """
        with self.closing_cursor(mode=ExecutionMode.SNAPSHOT_DDL) as cur:
            cur.execute(statement_text, parameters, timeout=timeout, statement_name=statement_name)

        # Return the last version of the statement
        return cur.statement

    def execute_streaming_ddl(
        self,
        statement_text: str,
        parameters: tuple | list | None = None,
        timeout: int = 3000,
        statement_name: str | None = None,
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
        Returns:
            Statement for any further management of the statement lifecycle
        """

        with self.closing_cursor(mode=ExecutionMode.STREAMING_DDL) as cur:
            cur.execute(statement_text, parameters, timeout=timeout, statement_name=statement_name)

        return cur.statement

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

    def _execute_statement(
        self,
        statement: str,
        execution_mode: ExecutionMode,
        statement_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute a SQL statement and return the response.

        Args:
            statement: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to 'dbapi-{uuid}')

        Returns:
            Dictionary containing the API response

        Raises:
            OperationalError: If statement execution fails
        """

        # Create the statement payload as per Flink SQL API documentation
        if statement_name is None:
            statement_name = f"dbapi-{str(uuid.uuid4())}"

        # Each connection uses a single environment, also
        # called catalog, so we set the property here
        properties = {"sql.current-catalog": self.environment}

        if self._dbname is not None:
            properties["sql.current-database"] = self._dbname

        if execution_mode.is_snapshot:
            # Ask for snapshot mode behavior -- point-in-time results.
            properties["sql.snapshot.mode"] = "now"

        payload = {
            "name": statement_name,
            "organization_id": self.organization_id,
            "environment_id": self.environment,
            "spec": {
                "statement": statement,
                "properties": properties,
                "compute_pool_id": self.compute_pool_id,
                "stopped": False,
            },
        }

        # Submit statement using the API
        res = self._request("/statements", method="POST", json=payload)
        return res.json()

    def _get_statement(self, statement_name: str) -> dict[str, Any]:
        """
        Get the current structure of a statement.

        Args:
            statement_name: The name of the statement to check

        Returns:
            Dictionary containing the statement status and details

        Raises:
            OperationalError: If status check fails
        """
        return self._request(f"/statements/{statement_name}").json()

    def _get_statement_results(
        self, statement_name: str, next_url: str | None
    ) -> tuple[list[ChangelogRow], str | None]:
        """
        Get a page of results for a statement.

        Args:
            statement_name: The name of the statement
            next_url: Optional full URL to fetch the next page of results from. If None, then
                        the results endpoint for the statement will be used.

        Returns:
            A 2-tuple: (list of results in changelog row format, optional url to fetch next page.)
            If the next page URL is None, there are no more pages to fetch.

        Raises:
            OperationalError: If results retrieval fails
        """
        if next_url is None:
            next_url = f"/statements/{statement_name}/results"

        response = self._request(next_url).json()

        # Promote from the pure from-response-json 'data' sub-member list of dicts
        # to a list of ChangelogRow.
        results: list[ChangelogRow] = [
            # 'op' may be omitted, in which case we assume 0 (INSERT)
            ChangelogRow(r.get("op", 0), r["row"])
            for r in response.get("results", {}).get("data", [])
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
            raise OperationalError(f"Error sending request {e.response.status_code}") from e


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
