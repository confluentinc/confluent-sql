import os
import uuid

import confluent_sql

"""Example of using the streaming execution mode with a dict-returning cursor."""

conn = confluent_sql.connect(
    flink_api_key=os.getenv("CONFLUENT_FLINK_API_KEY", ""),
    flink_api_secret=os.getenv("CONFLUENT_FLINK_API_SECRET", ""),
    environment=os.getenv("CONFLUENT_ENV_ID", ""),
    organization_id=os.getenv("CONFLUENT_ORG_ID", ""),
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID", ""),
    cloud_provider=os.getenv("CONFLUENT_CLOUD_PROVIDER", ""),
    cloud_region=os.getenv("CONFLUENT_CLOUD_REGION", ""),
    database=os.getenv("CONFLUENT_TEST_DBNAME", "default"),
)


# First, create a table with some sample data to query against. This will end up creating a Kafka
# topic and an Avro schema with three records in it.
table_name = f"sample_table_{uuid.uuid4().hex[:8]}"

with conn.closing_cursor() as cursor:
    print(f"Creating table {table_name} ...")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING
        )
        """
    )
    print(f"Inserting sample data into {table_name} ...")
    cursor.execute(
        f"""
        INSERT INTO {table_name} (id, name) VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
        """
    )

print("Querying from table in streaming mode with a dict cursor ...")
with conn.closing_streaming_cursor(as_dict=True) as cursor:
    # Execute a streaming (never-ending) query against the table we just created.
    # This will return a streaming cursor that we can iterate over.
    names_observed: set[str] = set()
    cursor.execute(f"SELECT id, name FROM {table_name}")
    # This query will result in an append-only changelog stream, so we won't need to bother with
    # a changelog compressor.
    assert cursor.statement.is_append_only is True, "Expected statement to be append-only"
    for row in cursor:
        assert isinstance(row, dict), f"Expected row to be a dict, got {type(row)}"
        names_observed.add(row["name"])
        print(f"Observed row in streaming query: {row}")

        # Once we've observed all three names, we can break out of the loop and end the query.
        #
        # (Note: this is a toy example to demonstrate streaming query behavior. In a real
        # application, you would likely have a more robust way of determining when to stop
        # consuming from the streaming cursor (if ever). Because no additional records will
        # be added to the source table/topic, we happen to know exactly when we've reached
        # the end of the stream as far as we're concerned, even though it's technically
        # a streaming query)
        if names_observed == {"Alice", "Bob", "Charlie"}:
            print("Observed all expected names, breaking out of streaming query loop.")
            break

# Clean up the table we created for this example.
with conn.closing_cursor() as cursor:
    print(f"Dropping table {table_name} ...")
    cursor.execute(f"DROP TABLE {table_name}")

print("Done.")
