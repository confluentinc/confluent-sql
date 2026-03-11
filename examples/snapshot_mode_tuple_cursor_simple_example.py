import os

import confluent_sql

"""Example of using the default snapshot execution mode with a tuple-returning cursor."""

conn = confluent_sql.connect(
    flink_api_key=os.getenv("CONFLUENT_FLINK_API_KEY"),
    flink_api_secret=os.getenv("CONFLUENT_FLINK_API_SECRET"),
    environment=os.getenv("CONFLUENT_ENV_ID"),
    organization_id=os.getenv("CONFLUENT_ORG_ID"),
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
    cloud_provider=os.getenv("CONFLUENT_CLOUD_PROVIDER"),
    cloud_region=os.getenv("CONFLUENT_CLOUD_REGION"),
)
cursor = conn.cursor()
try:
    # By default, statements submitted will be run in Flink "snapshot" mode, which
    # delivers point-in-time, finite results, matching the behavior of a traditional database
    # and DBAPI in general. Also, by default, cursors will return tuples.
    cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")

    # We can use the cursor as an iterator:
    for row in cursor:
        print(f"iterating over cursor results: {row}")

    # We can call fetchone, fetchmany, or fetchall as well:
    cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
    print(f"cursor.fetchone(): {cursor.fetchone()}")

    cursor.execute(
        """
        SELECT *
        FROM (
        VALUES
            (1, 2, 3),
            (4, 5, 6),
            (7, 8, 9),
            (10, 11, 12),
            (13, 14, 15)
        ) AS t(test_value_1, test_value_2, test_value_3)
        """
    )
    # Fetch just the first two rows.
    print(f"cursor.fetchmany(2): {cursor.fetchmany(2)}")
    # Fetch the rest of the rows of the result set.
    print(f"cursor.fetchall(): {cursor.fetchall()}")

finally:
    cursor.close()
    conn.close()
