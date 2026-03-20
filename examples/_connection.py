"""Shared connection helper for examples.

All examples need the same environment variables to connect to Confluent Cloud.
This module provides a single `get_connection()` function so examples stay focused
on the integration they're demonstrating rather than connection boilerplate.

Required environment variables:
    CONFLUENT_FLINK_API_KEY
    CONFLUENT_FLINK_API_SECRET
    CONFLUENT_ENV_ID
    CONFLUENT_ORG_ID
    CONFLUENT_COMPUTE_POOL_ID
    CONFLUENT_CLOUD_PROVIDER   (e.g. "aws")
    CONFLUENT_CLOUD_REGION     (e.g. "us-east-2")
    CONFLUENT_TEST_DBNAME      (optional, the Kafka cluster / Flink database name)
"""

import os

import confluent_sql


def get_connection(*, dbname: str | None = None) -> confluent_sql.Connection:
    """Create and return a confluent-sql connection from environment variables."""
    return confluent_sql.connect(
        flink_api_key=os.environ["CONFLUENT_FLINK_API_KEY"],
        flink_api_secret=os.environ["CONFLUENT_FLINK_API_SECRET"],
        environment=os.environ["CONFLUENT_ENV_ID"],
        organization_id=os.environ["CONFLUENT_ORG_ID"],
        compute_pool_id=os.environ["CONFLUENT_COMPUTE_POOL_ID"],
        cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
        cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
        dbname=dbname or os.getenv("CONFLUENT_TEST_DBNAME"),
    )
