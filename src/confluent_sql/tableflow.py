"""
Tableflow types, request builders, and response parsers for the Confluent SQL DB-API driver.

These model the Tableflow Topic API (`/tableflow/v1/tableflow-topics`), which adds an Iceberg
or Delta materialization sink to the Kafka topic backing a Flink table. The request-side inputs
(storage specs, config, error-handling options) are frozen so a single value can be reused across
many `enable_tableflow` calls without risk of mutation. The response-side models mirror
`Statement`: raw `spec`/`status` retained, sub-structures parsed, a `.phase` convenience.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from enum import Enum
from typing import ClassVar

from .exceptions import InterfaceError, OperationalError
from .types import StrAnyDict


class Fields:
    """Wire field names for the Tableflow Topic API

    Plain string class attributes, not an Enum: `StrEnum` needs Python 3.11+ (newer than this
    package's floor, 3.10), and the older `class X(str, Enum)` mixin (used above for `TableFormat`/
    `TableflowPhase`) returns the qualified member name from `str()`/default formatting rather
    than the plain value unless a caller remembers `.value` -- a real risk given how pervasively
    these get used directly as dict keys, in f-strings, and in JSON payloads. Plain strings have
    no such gotcha.
    """

    ID = "id"
    DISPLAY_NAME = "display_name"
    STORAGE = "storage"
    TABLE_FORMATS = "table_formats"
    ENVIRONMENT = "environment"
    KAFKA_CLUSTER = "kafka_cluster"
    CONFIG = "config"
    SUSPENDED = "suspended"

    RETENTION_MS = "retention_ms"
    DATA_RETENTION_MS = "data_retention_ms"
    ERROR_HANDLING = "error_handling"
    # Deliberately unmodeled by TableflowTopicConfig (deprecated/read-only), but still
    # referenced by name by a caller masking them out of a diff.
    ENABLE_COMPACTION = "enable_compaction"
    ENABLE_PARTITIONING = "enable_partitioning"
    RECORD_FAILURE_STRATEGY = "record_failure_strategy"

    KIND = "kind"
    BUCKET_NAME = "bucket_name"
    PROVIDER_INTEGRATION_ID = "provider_integration_id"
    STORAGE_ACCOUNT_NAME = "storage_account_name"
    CONTAINER_NAME = "container_name"
    TABLE_PATH = "table_path"

    MODE = "mode"
    TARGET = "target"


class TableFormat(str, Enum):
    """A concrete table format a Tableflow topic materializes to.

    API-faithful: this is what responses name (`spec.table_formats`,
    `status.failing_table_formats[].format`) and the unit of a future single-format disable.
    """

    ICEBERG = "ICEBERG"
    DELTA = "DELTA"


def table_format_from_spec(value: object) -> TableFormat:
    """Parse a single wire table-format value, converting an unrecognized one to
    `OperationalError` right here -- narrower than catching broadly further up the parse tree,
    so a real bug elsewhere in parsing isn't mistaken for a malformed server response.
    """
    try:
        return TableFormat(value)
    except ValueError as e:
        raise OperationalError(f"Error parsing Tableflow table format {value!r}: {e}") from e


def normalize_table_formats(
    table_formats: TableFormat | str | Collection[TableFormat],
) -> list[str]:
    """Normalize the `enable_tableflow`/`update_tableflow` `table_formats` argument to the wire
    array.

    Accepts a single `TableFormat` (convenience for the common one-format case) or any collection
    of them, and orders the result canonically by `TableFormat` declaration order so the request
    body is deterministic. Any `str` -- including a `TableFormat` (a `str` subclass) or a bare
    `"ICEBERG"` -- is treated as the single-format case; a `str` is iterable, so failing to special
    case it would split it into characters. Duplicates are rejected rather than silently collapsed:
    a repeated format is a caller mistake, not an intent to enable it twice.

    Raises:
        InterfaceError: If no formats are given (the API requires at least one), a value does not
            name a known `TableFormat`, or a format is repeated.
    """
    raw = [table_formats] if isinstance(table_formats, str) else list(table_formats)
    if not raw:
        raise InterfaceError("table_formats must name at least one TableFormat")
    try:
        coerced = [TableFormat(fmt) for fmt in raw]
    except ValueError as e:
        raise InterfaceError(f"unknown table format in table_formats: {e}") from e

    chosen: set[TableFormat] = set()
    duplicates: set[str] = set()
    for fmt in coerced:
        if fmt in chosen:
            duplicates.add(fmt.value)
        chosen.add(fmt)
    if duplicates:
        raise InterfaceError(
            f"table_formats contains duplicate formats: {', '.join(sorted(duplicates))}"
        )
    return [fmt.value for fmt in TableFormat if fmt in chosen]


class TableflowPhase(str, Enum):
    """Lifecycle phase of a Tableflow topic.

    The API marks this an extensible enum, so an unrecognized value parses to `UNKNOWN` rather
    than raising -- a future server-side phase shouldn't break response parsing.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, value: object) -> TableflowPhase:
        return cls.UNKNOWN

    @property
    def is_terminal(self) -> bool:
        """True once polling can stop: the topic is healthy (RUNNING) or broken (FAILED).

        UNKNOWN is deliberately non-terminal -- a state we don't understand shouldn't end a wait;
        the caller's timeout governs that instead.
        """
        return self in (TableflowPhase.RUNNING, TableflowPhase.FAILED)


@dataclass(frozen=True)
class TableflowStorage:
    """Base for the storage backends (`spec.storage`, a oneOf discriminated on `kind`).

    Frozen so a single instance is safely reusable across many `enable_tableflow` calls.
    """

    kind: ClassVar[str]

    def to_spec(self) -> StrAnyDict:
        """Render the writable storage fields to the wire `spec.storage` object."""
        return {Fields.KIND: self.kind}


@dataclass(frozen=True)
class ManagedStorage(TableflowStorage):
    """Confluent-managed storage -- the zero-config backend."""

    kind: ClassVar[str] = "Managed"


@dataclass(frozen=True)
class ByobAwsStorage(TableflowStorage):
    """Bring-your-own-bucket storage on AWS S3."""

    kind: ClassVar[str] = "ByobAws"

    bucket_name: str
    provider_integration_id: str

    def to_spec(self) -> StrAnyDict:
        return {
            Fields.KIND: self.kind,
            Fields.BUCKET_NAME: self.bucket_name,
            Fields.PROVIDER_INTEGRATION_ID: self.provider_integration_id,
        }


@dataclass(frozen=True)
class AzureAdlsStorage(TableflowStorage):
    """Customer-owned Azure Data Lake Storage Gen2."""

    kind: ClassVar[str] = "AzureDataLakeStorageGen2"

    storage_account_name: str
    container_name: str
    provider_integration_id: str

    def to_spec(self) -> StrAnyDict:
        return {
            Fields.KIND: self.kind,
            Fields.STORAGE_ACCOUNT_NAME: self.storage_account_name,
            Fields.CONTAINER_NAME: self.container_name,
            Fields.PROVIDER_INTEGRATION_ID: self.provider_integration_id,
        }


@dataclass(frozen=True)
class TableflowStorageUnknown(TableflowStorage):
    """An unrecognized `spec.storage.kind` -- mirrors `TableflowPhase`'s `UNKNOWN` fallback so a
    future server-side storage kind doesn't break response parsing for an otherwise-healthy
    topic. `kind` holds whatever the server actually sent (an instance field here, unlike the
    other storage classes' fixed `ClassVar`); the other fields are unrecoverable since their
    shape isn't known.
    """

    kind: str


def storage_from_spec(data: StrAnyDict) -> TableflowStorage:
    """Parse a response `spec.storage` object into its typed storage class.

    Captures only the writable fields; server-assigned read-only fields (`table_path`,
    `bucket_region`, `storage_region`) remain available on the topic's raw spec dict.
    """
    if not isinstance(data, dict):
        # A present-but-null (or otherwise non-mapping) storage section: raise explicitly here,
        # rather than let a bare .get() below raise AttributeError and rely on a broad except
        # further up the parse tree to relabel it -- that would just as readily mask a real bug.
        raise OperationalError(f"Error parsing Tableflow storage: expected a mapping, got {data!r}")
    kind = data.get(Fields.KIND)
    if kind == ManagedStorage.kind:
        return ManagedStorage()
    if kind == ByobAwsStorage.kind:
        return ByobAwsStorage(
            bucket_name=data[Fields.BUCKET_NAME],
            provider_integration_id=data[Fields.PROVIDER_INTEGRATION_ID],
        )
    if kind == AzureAdlsStorage.kind:
        return AzureAdlsStorage(
            storage_account_name=data[Fields.STORAGE_ACCOUNT_NAME],
            container_name=data[Fields.CONTAINER_NAME],
            provider_integration_id=data[Fields.PROVIDER_INTEGRATION_ID],
        )
    return TableflowStorageUnknown(kind=kind)


@dataclass(frozen=True)
class TableflowErrorHandling:
    """Base for record-failure handling (`spec.config.error_handling`, oneOf on `mode`)."""

    mode: ClassVar[str]

    def to_spec(self) -> StrAnyDict:
        """Render to the wire `error_handling` object."""
        return {Fields.MODE: self.mode}


@dataclass(frozen=True)
class TableflowErrorHandlingSuspend(TableflowErrorHandling):
    """Suspend materialization on a bad record (the server default)."""

    mode: ClassVar[str] = "SUSPEND"


@dataclass(frozen=True)
class TableflowErrorHandlingSkip(TableflowErrorHandling):
    """Skip bad records and continue materializing."""

    mode: ClassVar[str] = "SKIP"


@dataclass(frozen=True)
class TableflowErrorHandlingLog(TableflowErrorHandling):
    """Log bad records to a dead-letter topic and continue materializing."""

    mode: ClassVar[str] = "LOG"

    target: str = "error_log"

    def to_spec(self) -> StrAnyDict:
        return {Fields.MODE: self.mode, Fields.TARGET: self.target}


@dataclass(frozen=True)
class TableflowErrorHandlingUnknown(TableflowErrorHandling):
    """An unrecognized `config.error_handling.mode` -- mirrors `TableflowPhase`'s `UNKNOWN`
    fallback so a future server-side mode doesn't break response parsing for an otherwise-healthy
    topic. `mode` holds whatever the server actually sent (an instance field here, unlike the
    other error-handling classes' fixed `ClassVar`).
    """

    mode: str


def error_handling_from_spec(data: StrAnyDict) -> TableflowErrorHandling:
    """Parse a response `config.error_handling` object into its typed error-handling class.

    Mirrors `storage_from_spec` -- only the mode-to-class dispatch is ours, and every mode's
    field shape already round-trips through its own dataclass.
    """
    if not isinstance(data, dict):
        raise OperationalError(
            f"Error parsing Tableflow error_handling: expected a mapping, got {data!r}"
        )
    mode = data.get(Fields.MODE)
    if mode == TableflowErrorHandlingSuspend.mode:
        return TableflowErrorHandlingSuspend()
    if mode == TableflowErrorHandlingSkip.mode:
        return TableflowErrorHandlingSkip()
    if mode == TableflowErrorHandlingLog.mode:
        return TableflowErrorHandlingLog(target=data.get(Fields.TARGET, "error_log"))
    return TableflowErrorHandlingUnknown(mode=mode)


@dataclass(frozen=True)
class TableflowTopicConfig:
    """Topic-level Tableflow config, shared across all enabled formats.

    Only the writable fields are modeled; `to_spec` emits only those actually set, so an empty
    config sends nothing. The deprecated `record_failure_strategy` and the read-only
    `enable_compaction`/`enable_partitioning` flags are deliberately omitted.
    """

    retention_ms: int | None = None
    data_retention_ms: int | None = None
    error_handling: TableflowErrorHandling | None = None

    @classmethod
    def from_spec(cls, data: StrAnyDict) -> TableflowTopicConfig:
        """Parse a response `spec.config` object, dropping anything not formally modeled."""
        if not isinstance(data, dict):
            raise OperationalError(
                f"Error parsing Tableflow config: expected a mapping, got {data!r}"
            )
        error_handling_conf = data.get(Fields.ERROR_HANDLING)
        return cls(
            retention_ms=optional_int_from_str(data.get(Fields.RETENTION_MS)),
            data_retention_ms=optional_int_from_str(data.get(Fields.DATA_RETENTION_MS)),
            error_handling=(
                error_handling_from_spec(error_handling_conf)
                if error_handling_conf is not None
                else None
            ),
        )

    def to_spec(self) -> StrAnyDict:
        """Render to the wire `config` object.

        `retention_ms`/`data_retention_ms` accept `int` here for caller convenience, but the API
        schema types both as `string` (`format: int64`) on every request and response.
        """
        spec: StrAnyDict = {}
        if self.retention_ms is not None:
            spec[Fields.RETENTION_MS] = str(self.retention_ms)
        if self.data_retention_ms is not None:
            spec[Fields.DATA_RETENTION_MS] = str(self.data_retention_ms)
        if self.error_handling is not None:
            spec[Fields.ERROR_HANDLING] = self.error_handling.to_spec()
        return spec


def build_create_payload(
    *,
    table_name: str,
    table_formats: list[str],
    storage: TableflowStorage,
    config: TableflowTopicConfig | None,
    environment_id: str,
    kafka_cluster_id: str,
) -> StrAnyDict:
    """Assemble the `POST /tableflow/v1/tableflow-topics` request body.

    `table_name` is the Flink table, which is the backing Kafka topic name, which is
    `spec.display_name`. `table_formats` is the wire array from `normalize_table_formats`. An
    empty config is omitted entirely.
    """
    spec: StrAnyDict = {
        Fields.DISPLAY_NAME: table_name,
        Fields.STORAGE: storage.to_spec(),
        Fields.TABLE_FORMATS: table_formats,
        Fields.ENVIRONMENT: {Fields.ID: environment_id},
        Fields.KAFKA_CLUSTER: {Fields.ID: kafka_cluster_id},
    }
    if config is not None:
        config_spec = config.to_spec()
        if config_spec:
            spec[Fields.CONFIG] = config_spec
    return {"spec": spec}


def build_update_payload(
    *,
    table_formats: list[str] | None,
    config_spec: StrAnyDict | None,
    environment_id: str,
    kafka_cluster_id: str,
) -> StrAnyDict:
    """Assemble the `PATCH /tableflow/v1/tableflow-topics/{display_name}` request body.

    `table_formats`/`config_spec` are the only fields updatable via this API (`storage`/
    `display_name` are `x-immutable`; `suspended` isn't part of `tableflow`'s config surface) --
    `None` (or an empty `config_spec`) means "leave unchanged," so it's omitted from the body
    entirely rather than sent as `null`. Diffing to decide what's actually changing --
    comparing against a real `get_tableflow` response -- is the caller's job, not this driver's;
    this function (and `Connection.update_tableflow`) just assembles what it's given, same as
    every other Tableflow request-building function here.

    `environment` and `kafka_cluster` are both required routing/identity keys on this endpoint
    (the path only carries `display_name`, which isn't unique on its own) -- not values being
    changed. The API spec only marks `environment` required in the PATCH request schema, but
    that's wrong in practice: `kafka_cluster` is required here too, the same as it is for
    GET/DELETE.
    """
    spec: StrAnyDict = {
        Fields.ENVIRONMENT: {Fields.ID: environment_id},
        Fields.KAFKA_CLUSTER: {Fields.ID: kafka_cluster_id},
    }
    if table_formats is not None:
        spec[Fields.TABLE_FORMATS] = table_formats
    if config_spec:
        spec[Fields.CONFIG] = config_spec
    return {"spec": spec}


@dataclass
class FailingTableFormat:
    """A format that failed to materialize, with its error (`status.failing_table_formats`)."""

    format: TableFormat
    error_message: str

    @classmethod
    def from_response(cls, data: StrAnyDict) -> FailingTableFormat:
        return cls(
            format=table_format_from_spec(data["format"]), error_message=data["error_message"]
        )


@dataclass
class TableflowTopicStatus:
    """Parsed server-only status (`TableflowTopicStatus`); the raw dict is retained."""

    phase: TableflowPhase
    error_message: str | None
    failing_table_formats: list[FailingTableFormat]
    write_mode: str | None
    catalog_sync_statuses: list[StrAnyDict]
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> TableflowTopicStatus:
        failing = [
            FailingTableFormat.from_response(f) for f in (data.get("failing_table_formats") or [])
        ]
        return cls(
            phase=TableflowPhase(data.get("phase")),
            error_message=data.get("error_message") or None,
            failing_table_formats=failing,
            write_mode=data.get("write_mode"),
            catalog_sync_statuses=data.get("catalog_sync_statuses") or [],
            raw=data,
        )


@dataclass
class TableflowTopicSpec:
    """Parsed topic spec, in the same shape whether it came from a real GET/create response or
    was assembled locally to represent a desired state -- `table_formats`/`storage`/`config` are
    all typed either way. The raw spec dict is kept (mirroring `Statement`) for anything not
    formally modeled here.
    """

    display_name: str
    table_formats: list[TableFormat]
    storage: TableflowStorage
    config: TableflowTopicConfig | None
    environment_id: str | None
    kafka_cluster_id: str | None
    suspended: bool
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> TableflowTopicSpec:
        config_data = data.get(Fields.CONFIG)
        return cls(
            display_name=data[Fields.DISPLAY_NAME],
            table_formats=[
                table_format_from_spec(fmt) for fmt in data.get(Fields.TABLE_FORMATS, [])
            ],
            storage=storage_from_spec(data[Fields.STORAGE]),
            config=TableflowTopicConfig.from_spec(config_data) if config_data is not None else None,
            environment_id=(data.get(Fields.ENVIRONMENT) or {}).get(Fields.ID),
            kafka_cluster_id=(data.get(Fields.KAFKA_CLUSTER) or {}).get(Fields.ID),
            suspended=bool(data.get(Fields.SUSPENDED, False)),
            raw=data,
        )


@dataclass
class TableflowTopic:
    """A Tableflow topic as returned by the create/read endpoints.

    Mirrors `Statement`: parsed `spec`/`status`, raw `metadata` retained, and a `.phase`
    convenience reading `status.phase`. Holds no connection back-reference -- refresh via
    `Connection.get_tableflow`.
    """

    spec: TableflowTopicSpec
    status: TableflowTopicStatus
    metadata: StrAnyDict = field(repr=False)

    @property
    def phase(self) -> TableflowPhase:
        """The topic's lifecycle phase (convenience for `status.phase`)."""
        return self.status.phase

    @classmethod
    def from_response(cls, response: StrAnyDict) -> TableflowTopic:
        """Build a TableflowTopic from a `tableflow.v1.TableflowTopic` JSON response."""
        try:
            spec = TableflowTopicSpec.from_response(response["spec"])
            status = TableflowTopicStatus.from_response(response["status"])
            metadata = response.get("metadata", {})
        except KeyError as e:
            raise OperationalError(f"Error parsing Tableflow topic response, missing {e}.") from e
        return cls(spec=spec, status=status, metadata=metadata)


def optional_int_from_str(s: str | None) -> int | None:
    """Parse a wire string-encoded int64 value (see `TableflowTopicConfig.to_spec`) back to
    `int`, converting a malformed value to `OperationalError` right here -- narrower than
    catching broadly further up the parse tree, so a real bug elsewhere in parsing isn't
    mistaken for a malformed server response.
    """
    if s is None:
        return None
    try:
        return int(s)
    except (ValueError, TypeError) as e:
        raise OperationalError(f"Error parsing int value {s!r}: {e}") from e
