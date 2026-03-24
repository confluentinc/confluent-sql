# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased

### Added
  * New optional keyword parameter `properties: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution properties](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level properties for default catalog, database, and execution mode cannot be overridden by this parameter.

### Changed
  * Respelled the `connect()` parameter `dbname` to `database`.

## 0.1.x

Early access release of the driver.
