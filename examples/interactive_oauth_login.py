"""Example of interactive human-login OAuth (epic #150): sign in once via a browser instead of
supplying API keys.

Running this pops a browser to Confluent Cloud for a one-time interactive sign-in (the driver
listens on 127.0.0.1:26640 for the redirect); once you consent, the resulting login is reused for
every Connection this process opens -- one browser bounce per process, even under something like
dbt's multi-threaded mode. The control-plane token this login mints reaches Tableflow, Connect,
and CMK; the data-plane token reaches Flink -- one credential covering everything an API-key
connection would otherwise need up to three separate key pairs for.

organization_id is optional under auth="oauth": omitted, it's discovered from your login session
(your default Confluent Cloud organization); supplied, it scopes the login to that organization
instead.
"""

import os

import confluent_sql

conn = confluent_sql.connect(
    auth="oauth",
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"]
)
print(f"Signed in to Confluent Cloud for organization {conn.organization_id!r}")

cursor = conn.cursor()
try:
    cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
    for row in cursor:
        print(f"iterating over cursor results: {row}")
finally:
    cursor.close()
    conn.close()
