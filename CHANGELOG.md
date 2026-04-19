# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased


## 0.3.0, 2026-04-09

### Changed - Breaking
  * `connect()` / `Connection.__init__()`: Renamed `environment` parameter to `environment_id` to clarify that an environment ID (_not_ name) is expected. The internal attribute `Connection.environment` has also been renamed to `Connection.environment_id`. Update all calls from `connect(environment="env-123")` to `connect(environment_id="env-123")`. (#92)
  * `Cursor.execute()` and peers: Respelled and re-typed the `statement_label: str | None` parameter to be `statement_labels: list[str] | None` to allow multiple labels to be applied to a statement, including `HIDDEN_LABEL`.

### Added
  * New `Connection.get_statement(statement)` method to retrieve statement metadata by name or refresh a Statement object with the latest server state. Accepts either a statement name (string) or a Statement object. Returns a Statement object with current phase, schema, and execution traits. (#86)
  * New `StatementNotFoundError` exception, a subclass of `OperationalError`, raised by `Connection.get_statement(statement)` when attempting to retrieve a statement that does not exist. Provides programmatic access to the statement name via the `statement_name` attribute.
  * New constant `confluent_sql.HIDDEN_LABEL` used for driving `Cursor.execute()` to indicate that the statement should be hidden in default listings in Confluent Cloud UIs. This feature is intended to be used for minor queries, such as when investigating `INFORMATION_SCHEMA`.
  * Added documentation regarding use of `connect(endpoint=)` parameter to make use of private networking endpoints (README.md, docstrings).



## 0.2.0, 2026-03-26

### Changed
  * Respelled the `connect()` parameter `dbname` to `database`. The old spelling `dbname` is deprecated and will be removed in after one release cycle.
  * Class `SqlNone` now gracefully strips trailing `NOT NULL` constraints from type names (case-insensitively), so that `str(SqlNone("DATE NOT NULL"))` returns valid FlinkSQL `"cast (null as DATE)"`.
  * `connect()` is now keyword-only callable.
  * The `host` parameter for `Connection.__init__()` has been renamed to `endpoint`.
  * Clarified and improved documentation around Flink region API key use.

### Added
  * New optional keyword parameter `properties: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution properties](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level properties for default catalog, database, and execution mode cannot be overridden by this parameter.
  * New optional `endpoint` parameter on `connect()` and `Connection.__init__` to allow users to specify a custom Confluent Cloud API base endpoint (e.g., for private networking, staging, etc.). Mutually exclusive with (`cloud_provider`, `cloud_region`) -- either `endpoint` or (`cloud_provider`, `cloud_region`) must be provided. This replaces the `host` parameter in `Connection.__init__()`. (#66)

### Removed
  * The unused control-plane `api_key` and `api_secret` `connect()` parameters have been removed. The Flink Region API key params `flink_api_key` and `flink_api_secret` remain.

## 0.1.x

Early access release of the driver.
