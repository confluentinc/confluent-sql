# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased

### Added

- `StatementProperties` -- a frozen, keyword-only dataclass giving a typed, autocomplete-friendly
  way to set the curated statement options instead of hand-building a `sql.*` dict. Fields (all
  optional): `snapshot_write_mode`, `state_ttl` (a `timedelta`, rendered to a Flink duration such
  as `"3600 s"`), `scan_startup_mode`, `local_time_zone`, plus an `extra` dict escape hatch for
  options not yet modeled. Only set fields are emitted, so an instance never pins a default nor
  collides with the driver-owned overlay. The enum-typed fields also accept a bare `str` so a
  Flink value newer than this driver can still be passed; a wrong-property enum value, a field of
  the wrong Python type, or an `extra` key that duplicates a modeled field, raises at construction.
  `extra` is copied into a read-only mapping, so the frozen guarantee holds after construction too.
  Pass one anywhere a `properties=` dict is accepted (`Cursor.execute`, `execute_snapshot_ddl`,
  `execute_streaming_ddl`, ...); it is downgraded to a dict and validated identically. Adds the
  `ScanStartupMode` value enum (`earliest-offset`/`latest-offset`/`timestamp`/`specific-offsets`).
  (#163)
- New module `confluent_sql.statement_properties` gives statement `SET` options a discoverable,
  type-checkable face alongside the existing open-ended `PropertiesDict`. `Property` is a `str`
  enum of the `sql.*` option keys from the [SET-options reference](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html)
  (e.g. `Property.SNAPSHOT_WRITE_MODE`); `SnapshotWriteMode` (`default`/`fast-write`) and
  `SnapshotMode` (`now`/`off`) enumerate the fixed value sets, sharing the `PropertyValue` base.
  Members are `str` instances that compare, hash, JSON-serialize, and stringify as their wire
  string, so they drop straight into a `properties=` dict with no `.value` unwrapping. `PropertiesDict`
  now advertises `Property` keys and `PropertyValue` values. (#162)
- class `Connection` now has methods to enable / inspect / disable [Tableflow](https://www.confluent.io/product/tableflow/) materialization of the Kafka topic backing a Flink table (#117). Tableflow-enabled topics/tables can be snapshot queried in an optimized fashion.
  - `Connection.enable_tableflow(table_name, *, tableflow_formats, storage, config=None, wait_for_running=True, timeout=300)` adds an Iceberg/Delta sink. `tableflow_formats` takes a single `TableFormat` (e.g. `TableFormat.ICEBERG`) or a collection for several; `storage` is one of `ManagedStorage()` (zero-config), `ByobAwsStorage`, or `AzureAdlsStorage`; `config` is an optional `TableflowTopicConfig`. By default it blocks until the topic reaches `RUNNING`; pass `wait_for_running=False` to return as soon as the create is accepted (topic in `PENDING`). Raises `TableflowTopicAlreadyExistsError` if Tableflow is already enabled.
  - `Connection.get_tableflow(table_name)` returns the current `TableflowTopic` (phase, spec, status), raising `TableflowTopicNotFoundError` if Tableflow is not enabled for the topic.
  - `Connection.disable_tableflow(table_name, *, wait_for_removal=True, timeout=300)` tears the sink down (all-or-nothing in v1, no per-format disable). By default it blocks until the topic is confirmed gone; pass `wait_for_removal=False` to return as soon as the delete is accepted.
- `organization_id` is now optional in `connect()`/`Connection()` when a global API key is supplied: if omitted, it's inferred via `GET /org/v2/organizations` -- lazily, on first use of the connection (not at `connect()` time) -- and used when exactly one organization is visible to the key. Raises `OperationalError` on first use if zero or multiple organizations are visible. Unchanged (still required, validated eagerly by `connect()`) for a Flink-region-only key or a dedicated Tableflow/Connect key, neither of which has `/org/v2` reach. (#132)
- BYOIDC bearer-token authentication for the Flink data plane: `connect()`/`Connection()` now accept an `external_access_token` / `identity_pool_id` pair, letting you authenticate with a bearer token minted by your own OAuth/OIDC identity provider (an "external" token) instead of a Confluent API key + secret. The `external_access_token` name mirrors the Confluent Flink Table API plugin's `client.oauth.external-access-token` option. When supplied, every Flink request carries `Authorization: Bearer <external_access_token>` and `Confluent-Identity-Pool-Id: <identity_pool_id>`. The two are mutually exclusive with every API-key parameter and must be provided together. Scope is the Flink data plane only -- Confluent's authorization model accepts no external token on the control-plane routes this driver calls, so Tableflow, Connectors, and the CMK cluster-id lookup fail closed under BYOIDC (they require an API-key connection), and `organization_id` stays mandatory (there is no control-plane reach to infer it). The token is used verbatim with no refresh; an expired token starts failing requests (surfaced as `OperationalError`) and the connection must be re-opened with a fresh token. (#100)

### Fixed

- Every network-level transport failure from the Flink gateway (`httpx.ConnectError`, `httpx.ReadError`, `httpx.RemoteProtocolError`, timeouts, etc.) is now translated to `OperationalError` instead of leaking the raw `httpx` exception -- this applies uniformly to idempotent GETs (once #137's retry budget is exhausted) and to non-idempotent POST/PATCH/DELETE calls (statement submission, `stop_statement()`, `delete_statement()`), matching the DB-API v2 contract that every exception the driver raises is one of `confluent_sql`'s own `Error` subclasses. The original exception remains available via `__cause__`. (#138)

## 0.4.2, 2026-07-17

### Changed

- Snapshot queries on Confluent Cloud Flink SQL are now Generally Available. Removed the Early Access warning previously emitted on first creation of a snapshot-mode cursor, along with the Early Access advisories throughout the documentation (`README.md`, `ARCHITECTURE.md`, `DBAPI_EXTENSIONS.md`, `STREAMING.md`). (#119, #160)

## 0.4.1, 2026-07-07

### Fixed

- Idempotent GET requests (`list_statements()`'s page-fetch loop, `get_statement()`, and result-page fetching) now retry transient transport errors -- connection resets (`httpx.NetworkError`) and servers that close pooled connections without responding (`httpx.RemoteProtocolError`) -- up to 3 times with a short exponential backoff, instead of failing on the first blip. POST/PATCH/DELETE requests (statement submission, `stop_statement()`, `delete_statement()`) are deliberately left unretried, since re-issuing them after a connection reset could double-submit or double-mutate state. (#137)
- The same idempotent GET requests now also retry transient HTTP response statuses (429, 500, 502, 503, 504), not just transport-level exceptions -- a request that reaches Confluent Cloud but gets a momentarily-overloaded gateway response was previously failing on the first such response instead of being retried like a dropped connection. (#140)
- Increased the default HTTP timeout from 5 to 10s for safety. `connect()` and `Connection.__init__()` no longer accept explicit `None` for `http_timeout_secs`.

## 0.4.0, 2026-06-15

### Added

- Support for "Global" Confluent Cloud API keys: `confluent_sql.connect()` (and `Connection`) now accept `global_api_key` / `global_api_secret`. A Global key works against every route this driver touches, so it is preferred over a Flink Region key when both are supplied. `flink_api_key` / `flink_api_secret` remain supported and are now optional; at least one fully-specified pair must be provided. If both pairs are supplied, the Global pair is used (and the Flink pair ignored, with a warning); a half-specified pair (key without secret, or vice versa) is rejected. (#112)
- Support for "poolless Flink": `confluent_sql.connect()` now treats `compute_pool_id` as optional. Statements submitted w/o their own overriding `compute_pool_id` via a connection w/o a default compute pool id will make use of the default compute pool in the environment+cloud region (provisioning one if necessary). See [the Confluent documentation](https://docs.confluent.io/cloud/current/flink/concepts/compute-pools.html#default-compute-pools) for more details.
- New `Connection.stop_statement(statement, *, wait_for_stopped=True, timeout=300)` method to stop a running statement without deleting it, leaving the statement resource around for inspection (unlike `delete_statement()`, which also destroys it). Accepts a statement name or a `Statement` object. By default it blocks until the statement reaches `STOPPED`; pass `wait_for_stopped=False` to return as soon as the stop is accepted. A matching `Cursor.stop_statement()` stops the cursor's current statement. New `Statement.is_stopped`, `Statement.is_stopping`, and `Statement.stop_requested` properties expose the relevant state. (#61)

### Changed

- `Connection.list_statements()`:
  - New optional parameter `compute_pool_id` to list statements only in a single compute pool (otherwise environment-wide).
  - New optional parameter `name_contains: str` to filter statements server-side to those whose name contains the given substring (case-sensitive).
  - Existing parameter `label` is now optional.
  - The end result is that callers can now provide between zero and all of the possible kwargs to vary between 'no filtering at all, return all current statements in the environment' and 'apply all the possible filters as if ANDed together.'

## 0.3.1, 2026-05-21

### Added

- New `http_timeout_secs` parameter for `connect()` to let the caller control how long to wait in HTTP requests.

## 0.3.0, 2026-04-09

### Changed - Breaking

- `connect()` / `Connection.__init__()`: Renamed `environment` parameter to `environment_id` to clarify that an environment ID (_not_ name) is expected. The internal attribute `Connection.environment` has also been renamed to `Connection.environment_id`. Update all calls from `connect(environment="env-123")` to `connect(environment_id="env-123")`. (#92)
- `Cursor.execute()` and peers: Respelled and re-typed the `statement_label: str | None` parameter to be `statement_labels: list[str] | None` to allow multiple labels to be applied to a statement, including `HIDDEN_LABEL`.

### Added

- New `Connection.get_statement(statement)` method to retrieve statement metadata by name or refresh a Statement object with the latest server state. Accepts either a statement name (string) or a Statement object. Returns a Statement object with current phase, schema, and execution traits. (#86)
- New `StatementNotFoundError` exception, a subclass of `OperationalError`, raised by `Connection.get_statement(statement)` when attempting to retrieve a statement that does not exist. Provides programmatic access to the statement name via the `statement_name` attribute.
- New constant `confluent_sql.HIDDEN_LABEL` used for driving `Cursor.execute()` to indicate that the statement should be hidden in default listings in Confluent Cloud UIs. This feature is intended to be used for minor queries, such as when investigating `INFORMATION_SCHEMA`.
- Added documentation regarding use of `connect(endpoint=)` parameter to make use of private networking endpoints (README.md, docstrings).

## 0.2.0, 2026-03-26

### Changed

- Respelled the `connect()` parameter `dbname` to `database`. The old spelling `dbname` is deprecated and will be removed in after one release cycle.
- Class `SqlNone` now gracefully strips trailing `NOT NULL` constraints from type names (case-insensitively), so that `str(SqlNone("DATE NOT NULL"))` returns valid FlinkSQL `"cast (null as DATE)"`.
- `connect()` is now keyword-only callable.
- The `host` parameter for `Connection.__init__()` has been renamed to `endpoint`.
- Clarified and improved documentation around Flink region API key use.

### Added

- New optional keyword parameter `properties: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution properties](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level properties for default catalog, database, and execution mode cannot be overridden by this parameter.
- New optional `endpoint` parameter on `connect()` and `Connection.__init__` to allow users to specify a custom Confluent Cloud API base endpoint (e.g., for private networking, staging, etc.). Mutually exclusive with (`cloud_provider`, `cloud_region`) -- either `endpoint` or (`cloud_provider`, `cloud_region`) must be provided. This replaces the `host` parameter in `Connection.__init__()`. (#66)

### Removed

- The unused control-plane `api_key` and `api_secret` `connect()` parameters have been removed. The Flink Region API key params `flink_api_key` and `flink_api_secret` remain.

## 0.1.x

Early access release of the driver.
