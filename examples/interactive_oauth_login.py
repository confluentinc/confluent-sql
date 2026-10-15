"""Example of interactive human-login OAuth: sign in once via a browser instead of
supplying API keys.

Running this pops a browser to Confluent Cloud for a one-time interactive sign-in (the driver
listens on 127.0.0.1:26642 for the redirect); once you consent, the resulting login is reused for
every Connection this process opens -- one browser bounce per process, even under something like
dbt's multi-threaded mode.

organization_id is optional under auth="oauth": omitted, it's discovered from your login session
(your default Confluent Cloud organization); supplied, it scopes the login to that organization
instead.

To sign in against a non-production Confluent Cloud environment instead, import `STAG` or `DEVEL`
(`from confluent_sql.oauth.config import STAG`) and pass the additional keyword argument
`oauth_config=STAG` to `confluent_sql.connect()`.
"""

import os

import confluent_sql

conn = confluent_sql.connect(
    auth="oauth",
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
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
