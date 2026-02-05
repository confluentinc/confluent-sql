"""
This script is used to see actual error messages by the library.
We want error messages to be clear and to always point to the "correct" error cause.
The library should always raise one of our exceptions, bar any bug.
"""

import logging
import os

import confluent_sql

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    conn = confluent_sql.connect(
        flink_api_key=os.getenv("CONFLUENT_FLINK_API_KEY"),
        flink_api_secret=os.getenv("CONFLUENT_FLINK_API_SECRET"),
        environment=os.getenv("CONFLUENT_ENV_ID"),
        organization_id=os.getenv("CONFLUENT_ORG_ID"),
        compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
        cloud_provider=os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws"),
        cloud_region=os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2"),
    )
    try:
        print("==> Creating first cursor")
        cursor = conn.cursor()
        # This statement is invalid because we can't write 'as value' since value is a keyword
        statement = "SELECT 1 as value"
        print(f"==> Running a statement with a syntax error: '{statement}'")
        try:
            cursor.execute(statement)
        except Exception:
            logger.exception("==> Error: ", exc_info=True)
        print("==> Can you tell what the problem was?")
    finally:
        conn.close()
