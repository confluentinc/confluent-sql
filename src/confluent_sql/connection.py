"""
Connection module for Confluent SQL DB-API driver.

This module provides the connect function and Connection class for establishing
connections to Confluent SQL services.
"""

import uuid
import logging
from typing import Optional, Dict, Any, Union, Tuple, List, TYPE_CHECKING

import httpx

from .exceptions import InterfaceError, OperationalError


if TYPE_CHECKING:
    from .cursor import Cursor

logger = logging.getLogger(__name__)


def connect(
    flink_api_key: str,
    flink_api_secret: str,
    environment: str,
    compute_pool_id: str,
    organization_id: str,
    cloud_provider: str,
    cloud_region: str,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
) -> "Connection":
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
    )


class Connection:
    """
    A connection to a Confluent SQL service.

    This class represents a connection to a Confluent SQL service and provides
    methods for creating cursors and managing the connection lifecycle.
    """

    def __init__(
        self,
        flink_api_key: str,
        flink_api_secret: str,
        environment: str,
        compute_pool_id: str,
        organization_id: str,
        cloud_provider: str,
        cloud_region: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
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
            api_key: Confluent Cloud API key for general Confluent Cloud resources (optional)
            api_secret: Confluent Cloud API secret for general Confluent Cloud resources (optional)
            host: The base URL for Confluent Cloud API (optional)
            **kwargs: Additional connection parameters
        """
        self.environment = environment
        self.compute_pool_id = compute_pool_id
        self.organization_id = organization_id
        self.api_key = api_key
        self.api_secret = api_secret
        self._closed = False
        self._cursors: list[Cursor] = []

        # Create httpx client for making API calls
        host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
        base_url = f"{host}/sql/v1/organizations/{organization_id}/environments/{environment}"
        basic_auth = httpx.BasicAuth(username=flink_api_key, password=flink_api_secret)
        # Create httpx client for making API calls
        self._client = httpx.Client(
            auth=basic_auth,
            base_url=base_url,
            headers={"Content-Type": "application/json"},
        )

    def close(self) -> None:
        """
        Close the connection and free associated resources.

        This method closes all cursors associated with this connection
        and marks the connection as closed.
        """
        if not self._closed:
            for cursor in self._cursors:
                cursor.close()
            self._cursors.clear()
            self._closed = True
            self._client.close()
        else:
            logger.info("Trying to close a closed connection, ignoring")

    def cursor(self) -> "Cursor":
        """
        Create and return a new cursor object.

        Returns:
            A new Cursor object associated with this connection

        Raises:
            InterfaceError: If the connection is closed
        """
        if self._closed:
            raise InterfaceError("Connection is closed")

        from .cursor import Cursor

        cursor = Cursor(self)
        self._cursors.append(cursor)
        return cursor

    def _request(self, url, method="GET", raise_for_status=True, **kwargs) -> httpx.Response:
        try:
            response = self._client.request(method, url, **kwargs)
            if raise_for_status:
                response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            raise OperationalError(f"Error sending request {e.response.status_code}") from e
        except Exception as e:
            raise OperationalError("Error sending request") from e

    def execute_statement(
        self,
        statement: str,
        parameters: Optional[Union[Tuple, List, Dict]] = None,
        statement_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement and return the response.

        Args:
            statement: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)

        Returns:
            Dictionary containing the API response

        Raises:
            OperationalError: If statement execution fails
        """
        # Create the statement payload as per Flink SQL API documentation
        if statement_name is None:
            statement_name = f"dbapi-{str(uuid.uuid4())}"

        # TODO: apply parameters to the statement
        # TODO: add properties for snapshot queries

        payload = {
            "name": statement_name,
            "organization_id": self.organization_id,
            "environment_id": self.environment,
            "spec": {
                "statement": statement,
                "properties": {},
                "compute_pool_id": self.compute_pool_id,
                "stopped": False,
            },
        }

        # Submit statement using the API
        return self._request("/statements", method="POST", json=payload).json()

    def get_statement_status(self, statement_name: str) -> Dict[str, Any]:
        """
        Get the current status of a statement.

        Args:
            statement_name: The name of the statement to check

        Returns:
            Dictionary containing the statement status and details

        Raises:
            OperationalError: If status check fails
        """
        return self._request(f"/statements/{statement_name}").json()

    def get_statement_results(self, statement_name: str) -> list:
        """
        Get results for a completed statement.

        Args:
            statement_name: The name of the statement
            page_token: Optional page token for pagination

        Returns:
            Dictionary containing the results

        Raises:
            OperationalError: If results retrieval fails
        """
        next_url = f"/statements/{statement_name}/results"
        results = []
        while next_url:
            response = self._request(next_url).json()
            results.extend(response.get("results", {}).get("data", []))
            next_url = response.get("metadata", {}).get("next")
        return results

    def delete_statement(self, statement_name: str) -> None:
        """
        Delete a statement.

        Args:
            statement_name: The name of the statement to delete
        """
        response = self._request(
            f"/statements/{statement_name}", method="DELETE", raise_for_status=False
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if response.status_code != 404:
                raise OperationalError("Error deleting statement") from e
            # If the response if 404, it means we don't need to delete the statement.
            logger.info(f"Statement '{statement_name}' not found while deleting, ignoring")
