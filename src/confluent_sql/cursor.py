"""
Cursor module for Confluent SQL DB-API driver.

This module provides the Cursor class for executing SQL statements and
retrieving results from Confluent SQL services.
"""

import time
import math
import random
import uuid
from typing import Optional, List, Tuple, Any, Union, Dict
from .connection import Connection
from .exceptions import InterfaceError, DatabaseError, ProgrammingError, OperationalError


class Cursor:
    """
    A cursor for executing SQL statements and retrieving results.
    
    This class provides methods for executing SQL statements and fetching
    results from a Confluent SQL service connection.
    """
    
    def __init__(self, connection: Connection):
        """
        Initialize a new cursor.
        
        Args:
            connection: The Connection object this cursor is associated with
        """
        self.connection = connection
        self._closed = False
        self._description = None
        self._rowcount = -1
        self._arraysize = 1
        self._last_executed = None
        
        # Statement execution state
        self._statement_id = None
        self._statement_status = None
        self._result_schema = None
        self._result_traits = None
        self._all_results = []
        self._current_result_index = 0
        
    def execute(self, operation: str, parameters: Optional[Union[Tuple, List, Dict]] = None, timeout: int = 3000, statement_name: Optional[str] = None) -> "Cursor":
        """
        Execute a SQL statement.
        
        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            timeout: Maximum time to wait for statement completion in seconds (default: 3000)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)
            
        Returns:
            Self for method chaining
            
        Raises:
            InterfaceError: If the cursor is closed
            ProgrammingError: If the SQL statement is invalid
            OperationalError: If the statement cannot be executed
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")
        
        if not operation or not operation.strip():
            raise ProgrammingError("SQL statement cannot be empty")
        
        self._last_executed = operation
        self._statement_id = None
        self._statement_status = None
        self._result_schema = None
        self._result_traits = None
        self._all_results = []
        self._current_result_index = 0
        
        try:
            # Step 1: Submit the SQL statement
            self._submit_statement(operation, parameters, statement_name)
            
            # Step 2: Wait for statement to be ready (not in PENDING status)
            # in this loop, if FAILED or DEGRADED, raise an error
            self._wait_for_statement_ready(timeout)
            
            # Step 3: Set up cursor state for all successful statements
            # All queries are treated as streaming - database server handles result management
            # TODO: handle snapshot queries
            self._setup_cursor_state()
                    
        except Exception as e:
            if isinstance(e, (InterfaceError, ProgrammingError, OperationalError)):
                raise
            raise OperationalError(f"Failed to execute statement: {e}")
        
        return self
    
    
    def _wait_for_statement_ready(self, timeout: int) -> None:
        """
        Wait for statement to be ready (not in PENDING status).
        Uses exponential backoff with jitter to prevent thundering herd problems.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Raises:
            OperationalError: If polling times out or fails
        """
        start_time = time.time()
        base_delay = 1.0  # Start with 1 second
        max_delay = 30.0  # Maximum delay between polls
        current_delay = base_delay
        
        while time.time() - start_time < timeout:
            try:
                # Check statement status - this will raise OperationalError if status/phase is missing
                self._check_statement_status()
                
                # Statement is ready when it's not in PENDING status
                # Check for failure states first - exit immediately
                if self._statement_status in ["FAILED", "DEGRADED"]:
                    return
                
                # For bounded queries, wait for COMPLETED status
                if self._result_traits and self._result_traits.get("is_bounded") is True:
                    if self._statement_status in ["COMPLETED"]:
                        return
                else:
                    # For unbounded queries, RUNNING is acceptable
                    if self._statement_status in ["RUNNING"]:
                        return
                
                # Exponential backoff with jitter to prevent thundering herd
                jitter = random.uniform(0.75, 1.25)  # ±25% randomness
                actual_delay = current_delay * jitter
                time.sleep(actual_delay)
                current_delay = min(current_delay * 1.5, max_delay)
                
            except Exception as e:
                raise OperationalError(f"Failed to poll statement status: {e}")
        
        raise OperationalError(f"Statement submission timed out after {timeout} seconds")
    
    def _setup_cursor_state(self) -> None:
        """
        Set up cursor state after successful statement execution.
        This sets description and rowcount but does not parse results.
        All queries are treated as streaming - database server handles result management.
        """
        # Set rowcount for non-query statements
        if not self._result_schema:
            self._rowcount = 0
        else:
            self._rowcount = -1  # Will be set when results are fetched

        # Set description for query statements
        if self._result_schema:
            self._description = [(col["name"], col["type"]["type"], None, None, None, None, None) 
                               for col in self._result_schema]
        
        # Store additional useful metadata
        if hasattr(self, '_statement_name'):
            self._statement_name = getattr(self, '_statement_name', None)
        
        # Store execution context for debugging/tracing
        if hasattr(self, '_spec') and self._spec:
            self._compute_pool_id = self._spec.get('compute_pool_id')
            self._principal = self._spec.get('principal')
        
        # Store statement characteristics
        if self._result_traits:
            self._sql_kind = self._result_traits.get('sql_kind')
            self._is_bounded = self._result_traits.get('is_bounded')
            self._is_append_only = self._result_traits.get('is_append_only')
            self._connection_refs = self._result_traits.get('connection_refs', [])
    
    def _submit_statement(self, operation: str, parameters: Optional[Union[Tuple, List, Dict]] = None, statement_name: Optional[str] = None) -> None:
        """
        Submit a SQL statement for execution.
        
        Args:
            operation: The SQL statement to execute
            parameters: Parameters for the SQL statement (optional)
            statement_name: Optional name for the statement (defaults to DB-API UUID if not provided)
            
        Raises:
            OperationalError: If statement submission fails
        """
        try:
            # Delegate to connection object for statement execution
            response = self.connection.execute_statement(operation, parameters, statement_name)
            print(response)
            '''
            sample response from API:
            {
            "api_version": "sql/v1",
            "kind": "Statement",
            "metadata": {
                "self": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-123/statements/my-statement",
                "created_at": "1996-03-19T01:02:03-04:05",
                "updated_at": "2023-03-31T00:00:00-00:00",
                "uid": "12345678-1234-1234-1234-123456789012",
                "resource_version": "a23av",
                "labels": {
                "user.confluent.io/hidden": "true",
                "property1": "string",
                "property2": "string"
                },
                "resource_name": ""
            },
            "name": "sql123",
            "organization_id": "7c60d51f-b44e-4682-87d6-449835ea4de6",
            "environment_id": "string",
            "spec": {
                "statement": "SELECT * FROM TABLE WHERE VALUE1 = VALUE2;",
                "properties": {
                "sql.current-catalog": "my_environment",
                "sql.current-database": "my_kafka_cluster"
                },
                "compute_pool_id": "fcp-00000",
                "principal": "sa-abc123",
                "stopped": false
            },
            "status": {
                "phase": "RUNNING",
                "scaling_status": {
                "scaling_state": "OK",
                "last_updated": "1996-03-19T01:02:03-04:05"
                },
                "detail": "Statement is running successfully",
                "traits": {
                "sql_kind": "SELECT",
                "is_bounded": true,
                "is_append_only": true,
                "upsert_columns": [
                    0
                ],
                "schema": {
                    "columns": [
                    {
                        "name": "Column_Name",
                        "type": {
                        "type": "CHAR",
                        "nullable": true,
                        "length": 8
                        }
                    }
                    ]
                },
                "connection_refs": [
                    "my-postgres-connection",
                    "my-kafka-connection"
                ]
                },
                "network_kind": "PUBLIC",
                "latest_offsets": {
                "topic-1": "partition:0,offset:100;partition:1,offset:200",
                "topic-2": "partition:0,offset:50"
                },
                "latest_offsets_timestamp": "2023-03-31T00:00:00-00:00"
            },
            "result": {
                "api_version": "sql/v1",
                "kind": "StatementResult",
                "metadata": {
                "self": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-123/statements",
                "next": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-abc123/statements?page_token=UvmDWOB1iwfAIBPj6EYb",
                "created_at": "2006-01-02T15:04:05-07:00"
                },
                "results": {
                "data": [
                    {
                    "op": 0,
                    "row": [
                        "101",
                        "Jay",
                        [
                        null,
                        "abc"
                        ],
                        [
                        null,
                        "456"
                        ],
                        "1990-01-12 12:00.12",
                        [
                        [
                            null,
                            "Alice"
                        ],
                        [
                            "42",
                            "Bob"
                        ]
                        ]
                    ]
                    }
                ]
                }
            }
            }
        '''
            
            # Parse the response from connection
            self._statement_id = response.get("metadata", {}).get("uid")
            self._statement_name = response.get("name")
            
            # Store the full response components as object attributes
            self._spec = response.get("spec")
            self._status = response.get("status")
            self._result = response.get("result")
            
            # Handle status/phase extraction with proper error handling
            if self._status and self._status.get("phase"):
                self._statement_status = self._status["phase"]
            elif self._status:
                # Status exists but no phase - this is unexpected, treat as error
                raise OperationalError("Statement response missing phase information")
            else:
                # No status at all - this is unexpected, treat as error
                raise OperationalError("Statement response missing status information")
            
            # Extract result schema and traits if available (may be present in RUNNING/COMPLETED responses)
            if self._status and self._status.get("traits") and self._status["traits"].get("schema"):
                self._result_schema = self._status["traits"]["schema"]["columns"]
            
            if self._status and self._status.get("traits"):
                self._result_traits = self._status["traits"]
            
        except Exception as e:
            if isinstance(e, OperationalError):
                raise
            raise OperationalError(f"Failed to submit {operation}: {e}")
    
    def _parse_results_from_response(self, data: List[Dict]) -> None:
        """
        Parse results from the API response data and append to existing results.
        
        Args:
            data: List of result data from the API response
        """
        # Don't clear existing results - append to them
        # self._all_results = []  # REMOVED - this was overwriting previous results
        
        for item in data:
            # Extract operation and row data
            operation_code = item.get("op", 0)  # Default to 0 (insert)
            row_data = item.get("row", [])
            
            # Convert operation code to changelog operation
            operation_map = {
                0: "+I",  # Insert
                1: "-U",  # Update before
                2: "+U",  # Update after
                3: "-D",  # Delete
            }
            operation = operation_map.get(operation_code, "+I")
            
            # Create result row with operation and data
            result_row = {"operation": operation}
            
            # Add column data if we have schema
            if self._result_schema and len(row_data) == len(self._result_schema):
                for i, col in enumerate(self._result_schema):
                    result_row[col["name"]] = row_data[i]
            else:
                # Fallback: use generic column names
                for i, value in enumerate(row_data):
                    result_row[f"col_{i}"] = value
            
            self._all_results.append(result_row)
    
    def _check_statement_status(self) -> None:
        """
        Check the current status of the statement.
        
        Raises:
            OperationalError: If status check fails
        """
        try:
            # Delegate to connection object for status checking
            response = self.connection.get_statement_status(self._statement_name)

            '''
            sample response from API:
            {
                "api_version": "sql/v1",
                "kind": "Statement",
                "metadata": {
                    "self": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-123/statements/my-statement",
                    "created_at": "1996-03-19T01:02:03-04:05",
                    "updated_at": "2023-03-31T00:00:00-00:00",
                    "uid": "12345678-1234-1234-1234-123456789012",
                    "resource_version": "a23av",
                    "labels": {
                    "user.confluent.io/hidden": "true",
                    "property1": "string",
                    "property2": "string"
                    },
                    "resource_name": ""
                },
                "name": "sql123",
                "organization_id": "7c60d51f-b44e-4682-87d6-449835ea4de6",
                "environment_id": "string",
                "spec": {
                    "statement": "SELECT * FROM TABLE WHERE VALUE1 = VALUE2;",
                    "properties": {
                    "sql.current-catalog": "my_environment",
                    "sql.current-database": "my_kafka_cluster"
                    },
                    "compute_pool_id": "fcp-00000",
                    "principal": "sa-abc123",
                    "stopped": false
                },
                "status": {
                    "phase": "RUNNING",
                    "scaling_status": {
                    "scaling_state": "OK",
                    "last_updated": "1996-03-19T01:02:03-04:05"
                    },
                    "detail": "Statement is running successfully",
                    "traits": {
                    "sql_kind": "SELECT",
                    "is_bounded": true,
                    "is_append_only": true,
                    "upsert_columns": [
                        0
                    ],
                    "schema": {
                        "columns": [
                        {
                            "name": "Column_Name",
                            "type": {
                            "type": "CHAR",
                            "nullable": true,
                            "length": 8
                            }
                        }
                        ]
                    },
                    "connection_refs": [
                        "my-postgres-connection",
                        "my-kafka-connection"
                    ]
                    },
                    "network_kind": "PUBLIC",
                    "latest_offsets": {
                    "topic-1": "partition:0,offset:100;partition:1,offset:200",
                    "topic-2": "partition:0,offset:50"
                    },
                    "latest_offsets_timestamp": "2023-03-31T00:00:00-00:00"
                },
                "result": {
                    "api_version": "sql/v1",
                    "kind": "StatementResult",
                    "metadata": {
                    "self": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-123/statements",
                    "next": "https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-abc123/statements?page_token=UvmDWOB1iwfAIBPj6EYb",
                    "created_at": "2006-01-02T15:04:05-07:00"
                    },
                    "results": {
                    "data": [
                        {
                        "op": 0,
                        "row": [
                            "101",
                            "Jay",
                            [
                            null,
                            "abc"
                            ],
                            [
                            null,
                            "456"
                            ],
                            "1990-01-12 12:00.12",
                            [
                            [
                                null,
                                "Alice"
                            ],
                            [
                                "42",
                                "Bob"
                            ]
                            ]
                        ]
                        }
                    ]
                    }
                }
            }
            '''
            
            # Update the full response components as object attributes
            self._spec = response.get("spec")
            self._status = response.get("status")
            self._results = response.get("result")
            
            # Update status from the API response
            if self._status and self._status.get("phase"):
                self._statement_status = self._status["phase"]
            elif self._status:
                # Status exists but no phase - this is unexpected
                raise OperationalError("Status check response missing phase information")
            else:
                # No status at all - this is unexpected
                raise OperationalError("Status check response missing status information")

            # Update result schema and traits if available
            if self._status and self._status.get("traits") and self._status["traits"].get("schema"):
                self._result_schema = self._status["traits"]["schema"]["columns"]
            
            if self._status and self._status.get("traits"):
                self._result_traits = self._status["traits"]
                
        except Exception as e:
            raise OperationalError(f"Failed to check statement status: {e}")
    
    def _handle_failed_statement(self) -> None:
        """
        Handle a failed statement by retrieving detailed error information.
        
        Raises:
            DatabaseError: With detailed error message from the API
        """
        try:
            # Delegate to connection object to get statement details
            response = self.connection.get_statement_status(self._statement_name)
            
            # Extract error details from the response
            error_message = "Statement execution failed"
            if response.get("status") and response["status"].get("detail"):
                error_message = response["status"]["detail"]
            
            raise DatabaseError(f"Statement failed: {error_message}")
            
        except DatabaseError:
            # Re-raise DatabaseError as-is
            raise
        except Exception as e:
            raise DatabaseError(f"Statement failed: {e}")
    
        
    def fetchall(self) -> List[Tuple]:
        """
        Fetch all remaining rows from the result set.
        
        Returns:
            A list of tuples, where each tuple represents a row
            
        Raises:
            InterfaceError: If the cursor is closed or no result set is available
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")
        
        if not self._statement_id:
            raise InterfaceError("No result set available. Call execute() first.")
        
        # All queries are treated as streaming - check if statement is ready
        if self._statement_status in ["RUNNING"] and self._result_traits.get("is_bounded") is False:
            raise InterfaceError("Statement results cannot be fetched. Statement is unbounded.")
        elif self._statement_status in ["RUNNING"] and self._result_traits.get("is_bounded") is True:
            raise InterfaceError("Statement is not ready for result fetching.")
        elif self._statement_status not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")
        
        if not self._result_schema:
            # Non-query statement (INSERT, UPDATE, DELETE, etc.)
            return []
        
        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()
        
        # Convert results to tuples with changelog operation as first element
        result_tuples = []
        for row in self._all_results:
            if self._result_traits and self._result_traits.get("changelog"):
                # Include changelog operation as first element
                operation = row.get("operation", "+I")  # Default to insert
                values = [v for k, v in row.items() if k != "operation"]
                result_tuples.append((operation,) + tuple(values))
            else:
                # Regular result without changelog
                result_tuples.append(tuple(row.values()))
        
        self._rowcount = len(result_tuples)
        return result_tuples
    
    def _fetch_all_pages(self) -> None:
        """
        Fetch all pages of results using pagination.
        This is where result parsing actually happens (lazy evaluation).
        All queries are treated as streaming - database server handles result management.
        
        Raises:
            OperationalError: If fetching results fails
        """
        # If results are already parsed, don't fetch again
        if self._all_results:
            return
        
        try:
            # Start with the first page
            response = self.connection.get_statement_results(self._statement_name)
            self._parse_results_from_response(response.get("results", {}).get("data", []))
            
            # Check for pagination and fetch all remaining pages
            next_url = response.get("metadata", {}).get("next", "")
            while next_url and len(next_url) > 0:
                # Fetch the next page using the Connection method
                next_response = self.connection.get_statement_results_from_url(next_url)
                self._parse_results_from_response(next_response.get("results", {}).get("data", []))
                
                # Get the next URL for the next iteration
                next_url = next_response.get("metadata", {}).get("next", "")
                
        except Exception as e:
            print(f"Failed to fetch results from API: {e}")
            # If results API fails, return empty results
            self._all_results = []
    
    def fetchone(self) -> Optional[Tuple]:
        """
        Fetch the next row from the result set.
        
        Returns:
            A tuple representing the next row, or None if no more rows
            
        Raises:
            InterfaceError: If the cursor is closed or no result set is available
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")
        
        if not self._statement_id:
            raise InterfaceError("No result set available. Call execute() first.")
        
        # All queries are treated as streaming - check if statement is ready
        if self._statement_status not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")
        
        if not self._result_schema:
            # Non-query statement
            return None
        
        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()
        
        # Get the current result and advance the index
        if self._current_result_index < len(self._all_results):
            row = self._all_results[self._current_result_index]
            self._current_result_index += 1
            
            if self._result_traits and self._result_traits.get("changelog"):
                # Include changelog operation as first element
                operation = row.get("operation", "+I")
                values = [v for k, v in row.items() if k != "operation"]
                return (operation,) + tuple(values)
            else:
                # Regular result without changelog
                return tuple(row.values())
        
        return None
        
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """
        Fetch the next set of rows from the result set.
        
        Args:
            size: Number of rows to fetch (defaults to arraysize)
            
        Returns:
            A list of tuples, where each tuple represents a row
            
        Raises:
            InterfaceError: If the cursor is closed or no result set is available
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")
        
        if not self._statement_id:
            raise InterfaceError("No result set available. Call execute() first.")
        
        # All queries are treated as streaming - check if statement is ready
        if self._statement_status not in ["COMPLETED", "STOPPED", "RUNNING"]:
            raise InterfaceError("Statement is not ready for result fetching.")
        
        if not self._result_schema:
            # Non-query statement
            return []
        
        # If we haven't fetched results yet, fetch them now
        if not self._all_results:
            self._fetch_all_pages()
        
        fetch_size = size if size is not None else self._arraysize
        results = []
        
        # Get the next batch of results
        remaining = len(self._all_results) - self._current_result_index
        batch_size = min(fetch_size, remaining)
        
        for i in range(batch_size):
            row = self._all_results[self._current_result_index + i]
            
            if self._result_traits and self._result_traits.get("changelog"):
                # Include changelog operation as first element
                operation = row.get("operation", "+I")
                values = [v for k, v in row.items() if k != "operation"]
                results.append((operation,) + tuple(values))
            else:
                # Regular result without changelog
                results.append(tuple(row.values()))
        
        self._current_result_index += batch_size
        return results
        
    def close(self) -> None:
        """
        Close the cursor and free associated resources.
        
        This method marks the cursor as closed and releases any
        resources associated with it. It also attempts to delete
        any running statement to prevent orphaned jobs.
        """
        if not self._closed:
            # Try to delete any running statement
            if self._statement_id and self._statement_status not in ["COMPLETED", "STOPPED", "FAILED"]:
                try:
                    self._delete_statement()
                except Exception:
                    # Ignore errors during cleanup
                    pass
            
            self._closed = True
            self._description = None
            self._rowcount = -1
            self._statement_id = None
            self._statement_status = None
            self._result_schema = None
            self._result_traits = None
            self._all_results = []
            self._current_result_index = 0
    
    def _delete_statement(self) -> None:
        """
        Delete a running statement to prevent orphaned jobs.
        
        Raises:
            OperationalError: If statement deletion fails
        """
        try:
            # Delegate to connection object for statement deletion
            self.connection.delete_statement(self._statement_id)
            
            # Mark as deleted
            self._statement_status = "DELETED"
            
        except Exception as e:
            raise OperationalError(f"Failed to delete statement: {e}")
            
    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        Get the description of the result set columns.
        
        Returns:
            A list of tuples describing the columns, or None if no result set
        """
        return self._description
        
    @property
    def rowcount(self) -> int:
        """
        Get the number of rows affected by the last operation.
        
        Returns:
            The number of rows affected, or -1 if not applicable
        """
        return self._rowcount
        
    @property
    def arraysize(self) -> int:
        """
        Get or set the number of rows to fetch with fetchmany().
        
        Returns:
            The current arraysize value
        """
        return self._arraysize
        
    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """
        Set the number of rows to fetch with fetchmany().
        
        Args:
            value: The new arraysize value
        """
        self._arraysize = value
    
    @property
    def statement_name(self) -> Optional[str]:
        """
        Get the name of the current statement.
        
        Returns:
            The statement name, or None if no statement is active
        """
        return getattr(self, '_statement_name', None)
    
    @property
    def sql_kind(self) -> Optional[str]:
        """
        Get the SQL statement type (SELECT, INSERT, etc.).
        
        Returns:
            The SQL statement type, or None if not available
        """
        return getattr(self, '_sql_kind', None)
    
    @property
    def is_bounded(self) -> Optional[bool]:
        """
        Get whether the result set is bounded (finite).
        
        Returns:
            True if bounded, False if unbounded, None if not available
        """
        return getattr(self, '_is_bounded', None)
    
    @property
    def is_append_only(self) -> Optional[bool]:
        """
        Get whether the result set is append-only.
        
        Returns:
            True if append-only, False otherwise, None if not available
        """
        return getattr(self, '_is_append_only', None)
    
    @property
    def connection_refs(self) -> List[str]:
        """
        Get the external connections used by this statement.
        
        Returns:
            List of connection references, empty list if none
        """
        return getattr(self, '_connection_refs', [])
