import os, confluent_sql, logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2")

    conn = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
    )

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
        # We can use the cursor as an iterator:
        for row in cursor:
            print(row)
    finally:
        conn.close()
