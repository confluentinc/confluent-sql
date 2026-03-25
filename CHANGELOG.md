# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased

### Added
  * New optional keyword parameter `properties: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution properties](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level properties for default catalog, database, and execution mode cannot be overridden by this parameter.
  * New optional `host` parameter on `connect()` to allow users to specify a custom Confluent Cloud API endpoint (e.g., for private networking endpoints). This new parameter is mutually exclusive with `cloud_region` and `cloud_provider`. (#66)

### Changed
  * Respelled the `connect()` parameter `dbname` to `database`. The old spelling `dbname` is deprecated and will be removed in after one release cycle.
  * `connect()` is now keyword-only callable.

### Removed
  * The unused control-plane `api_key` and `api_secret` `connect()` parameters have been removed. The Flink regional API key params `flink_api_key` and `flink_api_secret` remain.


## 0.1.x

Early access release of the driver.
