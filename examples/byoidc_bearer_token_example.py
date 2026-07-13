import os

import confluent_sql

"""Example of BYOIDC authentication: authenticate to the Flink data plane with a bearer token
minted by your own OAuth/OIDC identity provider, instead of a Confluent API key + secret.

The driver stamps `Authorization: Bearer <external_access_token>` and `Confluent-Identity-Pool-Id`
on every Flink request. BYOIDC reaches the Flink data plane only -- Tableflow, Connectors, and the
CMK cluster-id lookup require an API-key connection. organization_id is mandatory (there is no
control-plane reach to infer it), and the token is used verbatim with no refresh (when it
expires, open a fresh connection with a fresh token).
"""

conn = confluent_sql.connect(
    external_access_token=os.getenv("CONFLUENT_EXTERNAL_ACCESS_TOKEN", ""),
    identity_pool_id=os.getenv("CONFLUENT_IDENTITY_POOL_ID", ""),
    environment_id=os.getenv("CONFLUENT_ENV_ID", ""),
    organization_id=os.getenv("CONFLUENT_ORG_ID", ""),
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID", ""),
    cloud_provider=os.getenv("CONFLUENT_CLOUD_PROVIDER", ""),
    cloud_region=os.getenv("CONFLUENT_CLOUD_REGION", ""),
)
cursor = conn.cursor()
try:
    cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
    for row in cursor:
        print(f"iterating over cursor results: {row}")
finally:
    cursor.close()
    conn.close()
