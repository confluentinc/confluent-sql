"""
This script is used to see actual error messages by the library.
We want error messages to be clear and to always point to the "correct" error cause.
The library should always raise one of our exceptions, bar any bug.
"""
import os, confluent_sql, logging


logger = logging.getLogger(__name__)

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def msg(message, color=Colors.OKGREEN):
    print(f"{color}==> {message}{Colors.ENDC}")

if __name__ == "__main__":
    msg("Getting env vars for the connection...")
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2")

    msg("Creating the connection...")
    conn = confluent_sql.connect(
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        environment=environment,
        organization_id=organization_id,
        compute_pool_id=compute_pool_id,
        cloud_region=cloud_region,
        cloud_provider=cloud_provider,
    )

    msg("Creating first cursor")
    cursor = conn.cursor()
    # This statement is invalid because we can't write 'as value' since value is a keyword
    statement = "SELECT 1 as value"
    msg(f"Running a statement with a syntax error: '{statement}'")
    try:
        cursor.execute(statement)
    except Exception:
        logger.exception("", exc_info=True)
