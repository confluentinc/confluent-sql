"""
Connection module for Confluent SQL DB-API driver.

This module provides the connect function and Connection class for establishing
connections to Confluent SQL services.
"""

import base64
import uuid
from typing import Optional, Dict, Any, Union, Tuple, List, TYPE_CHECKING

import httpx

from .exceptions import InterfaceError, OperationalError


if TYPE_CHECKING:
    from .cursor import Cursor


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
    host: Optional[str] = None,
    **kwargs: Any,
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
        host: The base URL for Confluent Cloud API (optional, will be constructed if not provided)
        **kwargs: Additional connection parameters

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
        host=host,
        **kwargs,
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
        host: Optional[str] = None,
        **kwargs: Any,
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
        self.flink_api_key = flink_api_key
        self.flink_api_secret = flink_api_secret
        self.environment = environment
        self.compute_pool_id = compute_pool_id
        self.organization_id = organization_id
        self.cloud_provider = cloud_provider
        self.cloud_region = cloud_region
        self.api_key = api_key
        self.api_secret = api_secret
        self._closed = False
        self._cursors = []

        # Construct the base URL for the Flink SQL API
        if host:
            self.host = host
        else:
            # Use the correct URL structure that matches the Flink SQL API documentation
            self.host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"

        # Create Basic auth credentials for Flink API
        credentials = f"{self.flink_api_key}:{self.flink_api_secret}"
        self.basic_auth = base64.b64encode(credentials.encode()).decode()

        # Create httpx client for making API calls
        self._client = httpx.Client(
            base_url=self.host,
            headers={
                "Authorization": f"Basic {self.basic_auth}",
                "Content-Type": "application/json",
            },
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
            if hasattr(self, "_client"):
                self._client.close()

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
        try:
            # Create the statement payload as per Flink SQL API documentation
            if statement_name is None:
                # Generate a DB-API compliant UUID for the statement name
                statement_name = f"dbapi-{str(uuid.uuid4())}"

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
            url = f"{self.host}/sql/v1/organizations/{self.organization_id}/environments/{self.environment}/statements"
            response = self._client.post(url, json=payload)

            if response.status_code not in [200, 201]:
                raise OperationalError(
                    f"Failed to create statement: HTTP {response.status_code} - {response.text}"
                )

            return response.json()

        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to execute statement: {e}")

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
        try:
            url = f"{self.host}/sql/v1/organizations/{self.organization_id}/environments/{self.environment}/statements/{statement_name}"
            response = self._client.get(url)

            if response.status_code != 200:
                raise OperationalError(
                    f"Failed to get statement status: HTTP {response.status_code} - {response.text}"
                )

            return response.json()

        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to get statement status: {e}")

    def get_statement_results(
        self, statement_name: str, page_token: Optional[str] = None
    ) -> Dict[str, Any]:
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
        try:
            url = f"{self.host}/sql/v1/organizations/{self.organization_id}/environments/{self.environment}/statements/{statement_name}/results"

            params = {}
            if page_token:
                params["page_token"] = page_token

            response = self._client.get(url, params=params)

            if response.status_code != 200:
                raise OperationalError(
                    f"Failed to fetch results: HTTP {response.status_code} - {response.text}"
                )

            return response.json()

        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to get statement results: {e}")

    def get_statement_results_from_url(self, url: str) -> Dict[str, Any]:
        """
        Get results for a statement from a specific URL (for pagination).

        Args:
            url: The full URL to fetch results from

        Returns:
            Dictionary containing the results

        Raises:
            OperationalError: If results retrieval fails
        """
        try:
            response = self._client.get(url)

            if response.status_code != 200:
                raise OperationalError(
                    f"Failed to fetch results from URL: HTTP {response.status_code} - {response.text}"
                )

            return response.json()

        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to get statement results from URL: {e}")

    def delete_statement(self, statement_name: str) -> None:
        """
        Delete a statement.

        Args:
            statement_name: The name of the statement to delete

        Raises:
            OperationalError: If statement deletion fails
        """
        try:
            url = f"{self.host}/sql/v1/organizations/{self.organization_id}/environments/{self.environment}/statements/{statement_name}"
            response = self._client.delete(url)

            if response.status_code != 204:
                raise OperationalError(
                    f"Failed to delete statement: HTTP {response.status_code} - {response.text}"
                )

        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to delete statement: {e}")
