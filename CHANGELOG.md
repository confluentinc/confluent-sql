# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased

### Changed
  * Respelled the `connect()` parameter `dbname` to `database`. The old spelling `dbname` is deprecated and will be removed in after one release cycle.
  * `connect()` is now keyword-only callable.
  * The `host` parameter for `Connection.__init__()` has been renamed to `endpoint`.

### Added
  * New optional keyword parameter `properties: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution properties](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level properties for default catalog, database, and execution mode cannot be overridden by this parameter.
  * New optional `endpoint` parameter on `connect()` and `Connection.__init__` to allow users to specify a custom Confluent Cloud API base endpoint (e.g., for private networking, staging, etc.). Mutually exclusive with (`cloud_provider`, `cloud_region`) -- either `endpoint` or (`cloud_provider`, `cloud_region`) must be provided. This replaces the `host` parameter in `Connection.__init__()`. (#66)

### Removed
  * The unused control-plane `api_key` and `api_secret` `connect()` parameters have been removed. The Flink regional API key params `flink_api_key` and `flink_api_secret` remain.

## 0.1.x

Early access release of the driver.
