# confluent-sql

A DB-API v2 compliant Python driver for Confluent Cloud Flink SQL services.

## Overview

The `confluent-sql` library provides a standard DB-API v2 interface for connecting to and
executing SQL queries against Confluent Cloud Flink SQL services. This allows you to use
familiar database programming patterns with Confluent's streaming SQL capabilities.

## Status

This is pre-production code mainly developed as the lower level portion of a `dbt` adaptor for Confluent Cloud Flink, but is aimed to also be a reasonable standalone dbapi+ driver for python programs to interact with Confluent Flink SQL.

The behavior of snapshot-mode cursors, complying with dbapi semantics, are well stable. The streaming query extensions are more of a work in progress at this time. Feedback and suggestions are welcome!

The driver defaults to [snapshot mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html) for all queries unless streaming mode is explicitly requested.

## Prerequisites

- **Confluent Cloud account** with Flink environment
- **Existing Flink Database** (Confluent Cloud Kafka cluster)
- **API credentials**, one of:
  - an **interactive browser login**, with `auth="oauth"` (see [Interactive login](#interactive-browser-login) below), or
  - a **"Global" Confluent Cloud API key** and secret, passed as `global_api_key` / `global_api_secret`, or
  - a **Flink Region API key** and secret, passed as `flink_api_key` / `flink_api_secret`, or
  - a **BYOIDC bearer token** minted by your own OAuth/OIDC identity provider, passed as `external_access_token` / `identity_pool_id` (see [BYOIDC bearer-token authentication](#byoidc-bearer-token-authentication) below).

  The interactive browser login is the simplest way to get started, at the cost of one browser sign-in per process. A Global key works against every route this driver touches. A Flink Region key only works against the core Flink SQL routes (the main DB-API functionality), not other features such as Tableflow, Connectors, or Artifacts.
- **Organization ID** — required with a Flink Region API key or a BYOIDC bearer token, neither of which can discover it. With a Global API key it may be omitted if the key can see exactly one organization, in which case it's inferred automatically on first use of the connection. With `auth="oauth"` it may always be omitted and defaults to your login's organization.

### How to Obtain a Flink Region API Key

A **Flink Region API key** (also called a Flink SQL API key) is specific to your Flink region/environment (the environment + cloud provider + cloud region triplet) and provides access to the regional Flink SQL API endpoints. This is distinct from a Confluent Cloud control-plane API key. (A "Global" Confluent Cloud API key is the alternative — see above — and is created the same way but without scoping to a single Flink region.)

To create or find a Flink Region API key:

1. Go to [https://confluent.cloud/settings/api-keys](https://confluent.cloud/settings/api-keys)
2. Filter by resource **'Flink Region'**
3. Find an existing key, or follow the **'+ Add API Key'** path to create a new one
4. When creating a new key, select either:
   - **My account** - for development/testing
   - **Service account** - for production applications (recommended)
     and then:
   - **The Environment, Cloud Provider and Cloud Region** matching the Flink database(s) / Kafka cluster(s) you intend to use this driver against.
5. Save both the **API key** and **API secret** securely (the secret cannot be retrieved later)
6. Use these credentials as the `flink_api_key` and `flink_api_secret` parameters in the `connect()` function

API keys may also be generated using the Confluent CLI or by API access, outside the scope of this document.

### Interactive browser login

Instead of API keys, sign in once through your browser:

```python
conn = confluent_sql.connect(
    auth="oauth",
    environment_id="env-...",
    cloud_provider="aws",
    cloud_region="us-east-1",
)
```

The browser opens once per process; the login is shared by every connection the process opens. `organization_id` is optional and defaults to your login's organization. See [examples/interactive_oauth_login.py](examples/interactive_oauth_login.py). The resulting login can reach everything your user account can, so all of the driver's functionality, including Tableflow, Connectors, and Artifacts, is available.

**Callback port.** To catch the browser's redirect after you sign in, the driver listens on `127.0.0.1:26642` (path `/callback-confluent-sql-docs`) while the login is in progress. This port is fixed by the driver's registered OAuth client and cannot be changed, so it must be free; if something else holds it, `connect()` raises `OAuthLoginError` with reason `PORT_IN_USE`. Only one login can be in progress per machine at a time. If you don't finish signing in within two minutes, it raises `OAuthLoginError` with reason `TIMED_OUT`.

**Login lifetime and signing out.** The login is held for the life of the process: closing a `Connection` does not sign you out, and a later `connect(auth="oauth", ...)` in the same process reuses it without another browser bounce. To explicitly discard the login (and its tokens), for example in a long-lived notebook kernel or server, call `confluent_sql.oauth_shutdown_all()`. The next `connect(auth="oauth", ...)` will then open the browser again. Connections that are still open keep working until their tokens can no longer be refreshed, so close them first.

**Session lifetime and re-authentication.** A login's refresh token has a hard ~8 hour absolute lifetime (and expires sooner if idle for too long or revoked), after which the session can no longer be silently refreshed. What happens next is controlled by the `reauth` argument to `connect()`:

- `reauth="auto"` (the default): the driver opens the browser for a fresh sign-in and retries the failed request. Every connection in the process shares the new login. This suits interactive use such as notebooks and scripts run by a person.
- `reauth="raise"`: the request raises `confluent_sql.ReauthenticationRequired` instead. Use this for unattended runs (a long batch job or dbt run started and left running), where nobody would be at the browser. Its `reason` is `ABSOLUTE_EXPIRY` (the ~8 hour wall) or `REFRESH_REJECTED` (idle expiry or revocation); catch it to abort or restart the job cleanly.

### BYOIDC bearer-token authentication

If your Confluent Cloud organization is configured with your own registered OAuth/OIDC identity provider, you can authenticate to the **Flink data plane** with a bearer token you mint yourself (an "external" token in Confluent's authorization vocabulary) rather than a Confluent API key. Pass the token and the identity-pool id that scopes it:

```python
import os
import confluent_sql

connection = confluent_sql.connect(
    external_access_token=os.environ["MY_IDP_BEARER_TOKEN"],
    identity_pool_id="pool-abc123",
    environment_id="env-...",
    organization_id="org-...",   # required under BYOIDC (see below)
    cloud_provider="aws",
    cloud_region="us-east-2",
    database="your-database-name",   # optional, and works under BYOIDC: it only sets the default
                                     # database, which needs no CMK cluster-id lookup
)
```

The driver stamps `Authorization: Bearer <external_access_token>` and `Confluent-Identity-Pool-Id: <identity_pool_id>` on every Flink request. The parameter name mirrors the Confluent Flink Table API plugin's `client.oauth.external-access-token` option. A runnable example is in [examples/byoidc_bearer_token_example.py](examples/byoidc_bearer_token_example.py).

Things to know:

- **`external_access_token` and `identity_pool_id` are a pair** and are **mutually exclusive** with every API-key parameter (`global_*`, `flink_*`, `tableflow_*`, `connect_*`). Supplying a bearer token alongside any API key is rejected.
- **Flink data plane only.** Confluent's authorization model does not accept a raw external token on any control-plane route this driver calls. So under BYOIDC the control-plane surfaces — Tableflow (`enable_tableflow` / `get_tableflow` / `disable_tableflow`), Connectors, and the CMK cluster-id lookup that resolves a `database` name to its `lkc-…` id — fail closed with a clear error. Use an API-key connection for those, and pass `database_kafka_cluster_id` to `connect()` if you need the driver to know the cluster id without the CMK lookup.
- **`organization_id` is required** under BYOIDC. Inferring it needs a control-plane call an external token can't make, so it must be supplied explicitly.
- **No token refresh.** The token is used verbatim on every request. When it expires, requests begin failing (surfaced as `OperationalError`) and the connection is effectively dead — open a fresh connection with a fresh token.

## Installation

```bash
# Using pip
pip install confluent-sql

# Using uv (recommended)
uv add confluent-sql
```

## Quick Start

Setup the connection:

```python
import confluent_sql

# Connect to Confluent Cloud Flink SQL
connection = confluent_sql.connect(
    organization_id="your-org-uuid",  # optional if using a Global key -- see Prerequisites
    environment_id="env-123456",
    cloud_provider="aws",
    cloud_region="us-east-2",
    flink_api_key="your-flink-api-key",  # or global_api_key=... for a "Global" Confluent Cloud key
    flink_api_secret="your-flink-api-secret",  # or global_api_secret=...
    database="your-database-name",
    compute_pool_id="lfcp-789012"  # optional; omit to use the environment's default compute pool
)
```

Create a cursor and run a point-in-time `SNAPSHOT` query:

```python
cursor = connection.cursor()
cursor.execute("SELECT customer_id, name FROM customers")
```

Fetch results using `fetchone()`, `fetchmany()` and `fetchall()`:

```python
print(cursor.fetchone())
print(cursor.fetchmany(2))
print(cursor.fetchall())
```

Fetch results using the cursor as an iterator:

```python
for row in cursor:
    print(row)
```

Clean up:

```python
cursor.close()
connection.close()
```

**Dictionary Result Rows:**

```python

...
cursor = connection.cursor(as_dict=True)
cursor.execute("SELECT customer_id, name, email FROM customers WHERE customer_id = %s", (123,))
row = cursor.fetchone()
print(row["customer_id"])  # Access by column name
```

**Streaming Queries:**

```python
import time

...

# Execute a streaming statement, runs and produces results indefinitely until
# we stop consuming its results or the statement is stopped or deleted via API ...
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))

while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            print(row)
    else:
        time.sleep(0.1)
```

## Parameterized Statement and Flink to Python Value Support

This driver supports all Flink types, some with caveats. Please consult [the type support documentation](https://github.com/confluentinc/confluent-sql/blob/main/TYPES.md) for more details.

## DB-API Extensions

This driver extends the standard DB-API v2 interface with additional features:

- **Dictionary result rows** - Access columns by name instead of position
- **Streaming cursors** - Non-blocking result consumption from continuous queries
- **Changelog compression** - Automatic state management for aggregations and joins
- **Statement lifecycle management** - Named statements, labels, and resource management
- **Statement properties** - Execution controls not expressible inline within the SQL statement
- **Type system** - Full support for all Flink SQL types including streaming-specific types
- **Performance monitoring** - Built-in fetch metrics and introspection

## Architecture & How It Works

The `confluent-sql` driver communicates with Confluent Cloud Flink SQL through HTTP-based APIs. Unlike in traditional databases, statements are **first-class entities** on the server with their own lifecycle,
allowing features like:

- **Named statements** - Identify and recover queries across connections
- **Persistent execution** - Statements survive connection close and can be resumed
- **Batch management** - Label related statements for group operations

For an in-depth explanation of the HTTP architecture and statement lifecycle,
see **[ARCHITECTURE.md](https://github.com/confluentinc/confluent-sql/blob/main/ARCHITECTURE.md)**.

### Complete Documentation

For comprehensive documentation of all DB-API extensions, see **[DBAPI_EXTENSIONS.md](https://github.com/confluentinc/confluent-sql/blob/main/DBAPI_EXTENSIONS.md)**.

For detailed streaming query guidance, see **[STREAMING.md](https://github.com/confluentinc/confluent-sql/blob/main/STREAMING.md)**.

For type support and examples, see **[TYPES.md](https://github.com/confluentinc/confluent-sql/blob/main/TYPES.md)**.

## Private Networking Considerations

By default, this driver uses the public Confluent Cloud API networking endpoint for the provided cloud provider and region. However, if the Flink database / Kafka cluster you intend to query requires private networking connectivity, then provide the appropriate Flink private networking base URL as the `endpoint` parameter
to `connect()` or `Connection.__init__()`. Refer to the Confluent Cloud [Flink private networking documentation](https://docs.confluent.io/cloud/current/flink/concepts/flink-private-networking.html) for more information on composing your endpoint URL.

Symptoms of using the public endpoint when private networking is required include:

- HTTP 429-related exceptions raised when submitting statements querying tables whose backing Kafka topics / clusters are configured for private networking only.
- Empty or surprisingly missing results when querying `INFORMATION_SCHEMA` or `SHOW TABLES`, due to silent filtering of private-networking-only tables/topics when querying the system catalog.

## Development

### Setup

```bash
# Clone repository
git clone <repository-url>
cd confluent-sql

# Install uv if needed
pip install uv

# Install dependencies
uv sync

# Install in development mode
uv pip install -e .
```

### Running Tests

Set required environment variables for integration tests.
If any of the variables is not set, integration tests will be skipped.

```bash
export CONFLUENT_ORG_ID="org-123456" # Optional if using a Global key that can see exactly one org.
export CONFLUENT_ENV_ID="env-123456"
export CONFLUENT_CLOUD_PROVIDER="aws"
export CONFLUENT_CLOUD_REGION="us-east-2"
export CONFLUENT_FLINK_API_KEY="your-key" # Flink Region API key for the above cloud/region ...
export CONFLUENT_FLINK_API_SECRET="your-secret" # and associated secret.
# Alternatively, a "Global" Confluent Cloud API key (used in preference to the Flink pair if both
# are set): export CONFLUENT_GLOBAL_API_KEY / CONFLUENT_GLOBAL_API_SECRET instead. With a Global
# key, CONFLUENT_ORG_ID may be omitted -- the integration suite infers it, same as connect() does.
export CONFLUENT_TEST_DBNAME="test-db" # A database/kafka cluster name within the above cloud/region.
export CONFLUENT_COMPUTE_POOL_ID="lfcp-789012" # Optional. If set, the integration suite runs against this pool; if unset, the suite runs against the environment's default pool. The driver treats it as optional at connect() either way.
```

Run tests:

```bash
uv run pytest
```
