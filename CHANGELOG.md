# Change Log

All notable changes to this dbapi driver will be documented in this file.

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
