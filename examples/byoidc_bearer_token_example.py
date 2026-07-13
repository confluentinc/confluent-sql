"""Example of BYOIDC authentication: authenticate to the Flink data plane with a bearer token
minted by your own OAuth/OIDC identity provider, instead of a Confluent API key + secret.

The driver stamps `Authorization: Bearer <external_access_token>` and `Confluent-Identity-Pool-Id`
on every Flink request. BYOIDC reaches the Flink data plane only -- Tableflow, Connectors, and the
CMK cluster-id lookup require an API-key connection. organization_id is mandatory (there is no
control-plane reach to infer it), and the token is used verbatim with no refresh (when it
expires, open a fresh connection with a fresh token).
"""

import os

import confluent_sql

# os.environ[...] on the required params so a missing var fails fast naming itself, rather than an
# empty string slipping through: an empty external_access_token is falsy, so the driver would never
# enter BYOIDC mode and would instead raise a misleading API-key/org-id error.
conn = confluent_sql.connect(
    external_access_token=os.environ["CONFLUENT_EXTERNAL_ACCESS_TOKEN"],
    identity_pool_id=os.environ["CONFLUENT_IDENTITY_POOL_ID"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),  # optional; None -> default pool
)
cursor = conn.cursor()
try:
    cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
    for row in cursor:
        print(f"iterating over cursor results: {row}")
finally:
    cursor.close()
    conn.close()
