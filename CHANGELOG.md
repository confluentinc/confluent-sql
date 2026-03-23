# Change Log

All notable changes to this dbapi driver will be documented in this file.

## Unreleased

### Added
  * New optional keyword parameter `properies: PropertiesDict | None` on `Cursor.execute()` and related methods to allow callers to provide [statement execution parameters](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#table-options). Note: connection or cursor-level parameters for default catalog, database, and execution mode cannot be overridden by this parameter.

## 0.1.x

Early access release of the driver.
