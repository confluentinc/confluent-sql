"""
Tableflow types, request builders, and response parsers for the Confluent SQL DB-API driver.

These model the Tableflow Topic API (`/tableflow/v1/tableflow-topics`), which adds an Iceberg
or Delta materialization sink to the Kafka topic backing a Flink table. The request-side inputs
(storage specs, config, error-handling options) are frozen so a single value can be reused across
many `TableflowApi.enable` calls without risk of mutation. The response-side models mirror
`Statement`: raw `spec`/`status` retained, sub-structures parsed, a `.phase` convenience.

`TableflowApi` (obtained from `Connection.get_tableflow_api()`) holds the HTTP + poll choreography,
peer to `ArtifactsApi` and `ConnectorApi`. `Connection` hands it a `Requester` for the Tableflow
surface, so `connection` imports `tableflow` and never the reverse.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from enum import Enum
from typing import ClassVar

from .context import BACKGROUND, Context
from .exceptions import (
    InterfaceError,
    OperationalError,
    TableflowTopicAlreadyExistsError,
    TableflowTopicNotFoundError,
)
from .polling import poll_until
from .service_client import Requester, StatusExceptions
from .types import StrAnyDict
from .utils import ExtensibleEnum

logger = logging.getLogger(__name__)


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
    """Normalize the `TableflowApi.enable`/`update` `table_formats` argument to the wire
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


class TableflowPhase(ExtensibleEnum):
    """Lifecycle phase of a Tableflow topic.

    The API marks this an extensible enum, so an unrecognized value parses to `UNKNOWN` rather
    than raising -- a future server-side phase shouldn't break response parsing.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

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

    Frozen so a single instance is safely reusable across many `TableflowApi.enable` calls.
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
class GcsStorage(TableflowStorage):
    """Customer-owned Google Cloud Storage.

    Named for the informal abbreviation (mirroring `AzureAdlsStorage`) rather than the API
    schema name `GoogleCloudStorageSpec`, which would stutter to `GoogleCloudStorageStorage`.
    The `kind` ClassVar still carries the exact wire discriminator (`"GoogleCloudStorage"`).
    """

    kind: ClassVar[str] = "GoogleCloudStorage"

    bucket_name: str
    provider_integration_id: str

    def to_spec(self) -> StrAnyDict:
        return {
            Fields.KIND: self.kind,
            Fields.BUCKET_NAME: self.bucket_name,
            Fields.PROVIDER_INTEGRATION_ID: self.provider_integration_id,
        }


@dataclass(frozen=True)
class TableflowStorageUnknown(TableflowStorage):
    """An unrecognized `spec.storage.kind` -- mirrors `TableflowPhase`'s `UNKNOWN` fallback so a
    future server-side storage kind doesn't break response parsing for an otherwise-healthy
    topic. `kind` is fixed to the "UNKNOWN" sentinel (same kind of symbol -- a `ClassVar[str]` --
    as every other storage class, so it stays valid to read polymorphically).

    `raw` holds the entire unparsed `spec.storage` object, not just `kind`: an unrecognized kind
    could carry any fields at all (the way `AzureAdlsStorage` has `container_name`,
    `ByobAwsStorage` has `bucket_name`, ...), and there's no way to know which of them matter for
    a kind we don't recognize -- so, unlike the other storage classes (which capture only their
    own known-writable fields), this one keeps everything rather than guess.
    """

    kind: ClassVar[str] = "UNKNOWN"

    raw: StrAnyDict

    def to_spec(self) -> StrAnyDict:
        return self.raw


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
    if kind == GcsStorage.kind:
        return GcsStorage(
            bucket_name=data[Fields.BUCKET_NAME],
            provider_integration_id=data[Fields.PROVIDER_INTEGRATION_ID],
        )
    return TableflowStorageUnknown(raw=data)


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
    topic. `mode` is fixed to the "UNKNOWN" sentinel (same kind of symbol -- a `ClassVar[str]` --
    as every other error-handling class, so it stays valid to read polymorphically).

    `raw` holds the entire unparsed `config.error_handling` object, not just `mode`: an
    unrecognized mode could carry any fields at all (the way `TableflowErrorHandlingLog` has
    `target`), and there's no way to know which of them matter for a mode we don't recognize --
    so, unlike the other error-handling classes, this one keeps everything rather than guess.
    """

    mode: ClassVar[str] = "UNKNOWN"

    raw: StrAnyDict

    def to_spec(self) -> StrAnyDict:
        return self.raw


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
    return TableflowErrorHandlingUnknown(raw=data)


@dataclass(frozen=True)
class TableflowTopicConfig:
    """Topic-level Tableflow config, shared across all enabled formats.

    Only the writable fields are modeled; `to_spec` emits only those actually set, so an empty
    config sends nothing. The deprecated `record_failure_strategy` and the read-only
    `enable_compaction`/`enable_partitioning` flags are deliberately omitted.
    """

    retention_ms: str | int | None = None
    data_retention_ms: str | int | None = None
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
    comparing against a real `TableflowApi.get` response -- is the caller's job, not this driver's;
    this function (and `TableflowApi.update`) just assembles what it's given, same as
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
    `TableflowApi.get`.
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


class TableflowApi:
    """Lifecycle choreography for Tableflow topics against the Tableflow Topic API.

    Obtained from `Connection.get_tableflow_api()`. Covers enable / get / update / disable,
    holding the stateful HTTP + polling work. Blocks by default per the repo's
    `wait_for_<settled-state>` convention, polling via `poll_until`.

    `request` sends to the Tableflow routes, authenticated for Tableflow; `resolve_kafka_cluster_id`
    returns the `lkc-…` id of the Kafka cluster backing the connection's database, called per use.
    """

    _TOPICS_PATH = "/tableflow/v1/tableflow-topics"
    """Control-plane base path for the Tableflow-topics resource."""

    def __init__(
        self,
        request: Requester,
        *,
        environment_id: str,
        resolve_kafka_cluster_id: Callable[[], str],
    ) -> None:
        self._request = request
        self._environment_id = environment_id
        self._resolve_kafka_cluster_id = resolve_kafka_cluster_id

    def enable(
        self,
        table_name: str,
        *,
        table_formats: TableFormat | str | Collection[TableFormat],
        storage: TableflowStorage,
        config: TableflowTopicConfig | None = None,
        wait_for_running: bool = True,
        timeout: float = 300,
    ) -> TableflowTopic:
        """Enable Tableflow on the Kafka topic backing a Flink table -- add an Iceberg/Delta sink.

        `POST /tableflow/v1/tableflow-topics`. The topic is created asynchronously; by default this
        blocks until it reaches RUNNING (mirroring `Connection.stop_statement`'s wait-by-default).

        Args:
            table_name: The Flink table, which is the backing Kafka topic name, which becomes
                spec.display_name. Passed straight through (no escaping/casing translation).
            table_formats: Which format(s) to materialize to. A single TableFormat (e.g.
                TableFormat.ICEBERG) for the common case, or a collection for both
                ({TableFormat.ICEBERG, TableFormat.DELTA}). Required; at least one.
            storage: The storage backend -- ManagedStorage() (zero-config), ByobAwsStorage,
                AzureAdlsStorage, or GcsStorage. Required.
            config: Optional topic-level config (retention, error-handling), shared across all
                enabled formats.
            wait_for_running: If True (default), poll until the topic reaches RUNNING, raising on
                FAILED. If False, return as soon as the create is accepted (topic in PENDING).
            timeout: Maximum seconds to wait when wait_for_running is True.

        Returns:
            The TableflowTopic -- RUNNING when waited (the default), otherwise the just-accepted
            topic (typically PENDING).

        Raises:
            InterfaceError: If table_formats is empty or names an unknown format.
            ProgrammingError: If no control-plane credential is available, or the cluster id
                can't be resolved without a global key or an interactive OAuth connection.
            TableflowTopicAlreadyExistsError: If Tableflow is already enabled (HTTP 409).
            OperationalError: On other API errors, on FAILED during a wait, or on wait timeout.
        """
        # Validate the formats argument before any network work, so a bad argument fails fast
        # rather than after the (possibly CMK-resolving) cluster-id lookup.
        wire_formats = normalize_table_formats(table_formats)
        kafka_cluster_id = self._resolve_kafka_cluster_id()
        payload = build_create_payload(
            table_name=table_name,
            table_formats=wire_formats,
            storage=storage,
            config=config,
            environment_id=self._environment_id,
            kafka_cluster_id=kafka_cluster_id,
        )
        logger.info(f"Enabling Tableflow for table '{table_name}'")
        response = self._request(
            self._TOPICS_PATH,
            method="POST",
            json=payload,
            status_exceptions={
                409: lambda _response: TableflowTopicAlreadyExistsError(
                    f"Tableflow is already enabled for table '{table_name}'",
                    table_name=table_name,
                )
            },
            error_prefix="Error enabling Tableflow",
        )

        topic = TableflowTopic.from_response(response.json())
        if wait_for_running:
            return self._wait_for_running(topic, timeout)
        return topic

    def get(self, table_name: str) -> TableflowTopic:
        """Read the current Tableflow state for a table's topic (phase and the rest of status).

        `GET /tableflow/v1/tableflow-topics/{display_name}`.

        Args:
            table_name: The Flink table / Kafka topic name (the {display_name} path segment).

        Returns:
            The full TableflowTopic (phase, spec, status).

        Raises:
            ProgrammingError: If the Kafka cluster id can't be resolved -- no
                database_kafka_cluster_id was supplied and CMK resolution is unavailable
                (no global key and no interactive OAuth connection).
            TableflowTopicNotFoundError: If Tableflow is not enabled for the topic (HTTP 404).
            OperationalError: On other API errors.
        """
        return self._get_topic(table_name, BACKGROUND)

    def _get_topic(self, table_name: str, ctx: Context) -> TableflowTopic:
        """`get`, bounded by `ctx`: the `Context` of the wait it is part of, or
        `BACKGROUND`."""
        response = self._request(
            f"{self._TOPICS_PATH}/{table_name}",
            params=self._tableflow_topic_params(),
            status_exceptions=self._not_found(table_name),
            error_prefix="Error reading Tableflow topic",
            ctx=ctx,
        )
        return TableflowTopic.from_response(response.json())

    def update(
        self,
        table_name: str,
        *,
        table_formats: TableFormat | str | Collection[TableFormat] | None = None,
        config: TableflowTopicConfig | None = None,
        wait_for_running: bool = True,
        timeout: float = 300,
    ) -> TableflowTopic:
        """Update an already-enabled Tableflow topic's `table_formats`/`config`.

        `PATCH /tableflow/v1/tableflow-topics/{display_name}`. `table_formats`/`config` are the
        only fields updatable this way -- `storage`/`display_name` are `x-immutable`; a caller
        needing to change either has no in-place path and must recreate the topic instead.

        `None` means "leave unchanged," for this method's own two arguments and for each of
        `config`'s own sub-fields (same convention `config.to_spec()` already uses for create) --
        not "clear it": none of `retention_ms`/`data_retention_ms`/`error_handling` can actually
        be unset via this API (each has a server-enforced default, and the server rejects an
        explicit null for any of them), so there's no supported way to request a delete, and this
        method doesn't attempt one. This method itself doesn't diff against the topic's current
        live state -- that's the caller's job (comparing against `get`'s response);
        it just sends whatever `table_formats`/`config` it's given, same as `enable`.

        Args:
            table_name: The Flink table / Kafka topic name (the {display_name} path segment).
            table_formats: New table_formats, replacing the full list -- or None to leave
                unchanged.
            config: New topic-level config -- or None to leave unchanged. Only fields actually
                set on it (not None) are changed; the rest are left as-is.
            wait_for_running: If True (default), poll until the topic reaches RUNNING, raising on
                FAILED. If False, return as soon as the update is accepted.
            timeout: Maximum seconds to wait when wait_for_running is True.

        Returns:
            The TableflowTopic -- RUNNING when waited (the default), otherwise the just-accepted
            topic (typically PENDING while formats/config changes roll out).

        Raises:
            InterfaceError: If table_formats and config are both None (nothing to update), or if
                table_formats is empty or names an unknown format.
            ProgrammingError: If no control-plane credential is available, or the cluster id
                can't be resolved without a global key.
            TableflowTopicNotFoundError: If Tableflow is not enabled for the topic (HTTP 404).
            OperationalError: On other API errors, on FAILED during a wait, or on wait timeout.
        """
        # Validate/normalize before any network work (including the possibly CMK-resolving
        # cluster-id lookup below), same as `enable`.
        wire_formats = normalize_table_formats(table_formats) if table_formats is not None else None
        config_spec = config.to_spec() if config is not None else None
        if not wire_formats and not config_spec:
            raise InterfaceError("update requires at least one of table_formats/config")

        payload = build_update_payload(
            table_formats=wire_formats,
            config_spec=config_spec,
            environment_id=self._environment_id,
            kafka_cluster_id=self._resolve_kafka_cluster_id(),
        )
        logger.info(f"Updating Tableflow for table '{table_name}'")
        response = self._request(
            f"{self._TOPICS_PATH}/{table_name}",
            method="PATCH",
            json=payload,
            status_exceptions=self._not_found(table_name),
            error_prefix="Error updating Tableflow topic",
        )

        topic = TableflowTopic.from_response(response.json())
        if wait_for_running:
            return self._wait_for_running(topic, timeout)
        return topic

    def disable(
        self,
        table_name: str,
        *,
        wait_for_removal: bool = True,
        timeout: float = 300,
    ) -> None:
        """Tear down Tableflow on a table's topic -- remove the sink.

        `DELETE /tableflow/v1/tableflow-topics/{display_name}`. All-or-nothing in v1 (no
        per-format disable). Deletion is asynchronous; by default this blocks until the topic is
        confirmed gone.

        Args:
            table_name: The Flink table / Kafka topic name (the {display_name} path segment).
            wait_for_removal: If True (default), poll `get` until it 404s, confirming the
                topic is gone. Leaving this True before a DROP TABLE keeps the drop from racing an
                active materialization. If False, return as soon as the DELETE is accepted.
            timeout: Maximum seconds to wait when wait_for_removal is True.

        Raises:
            ProgrammingError: If the Kafka cluster id can't be resolved -- no
                database_kafka_cluster_id was supplied and CMK resolution is unavailable
                (no global key and no interactive OAuth connection).
            TableflowTopicNotFoundError: If Tableflow was not enabled for the topic (HTTP 404).
            OperationalError: On other API errors, or on wait timeout.
        """
        logger.info(f"Disabling Tableflow for table '{table_name}'")
        self._request(
            f"{self._TOPICS_PATH}/{table_name}",
            method="DELETE",
            params=self._tableflow_topic_params(),
            status_exceptions=self._not_found(table_name),
            error_prefix="Error disabling Tableflow",
        )

        if wait_for_removal:
            self._wait_for_removal(table_name, timeout)

    def _tableflow_topic_params(self) -> dict[str, str]:
        """The environment + cluster query params the per-topic GET/DELETE routes require."""
        return {
            Fields.ENVIRONMENT: self._environment_id,
            f"spec.{Fields.KAFKA_CLUSTER}": self._resolve_kafka_cluster_id(),
        }

    def _wait_for_running(self, topic: TableflowTopic, timeout: float) -> TableflowTopic:
        """Poll until the topic reaches RUNNING, refreshing via `get`.

        Raises:
            OperationalError: If the topic transitions to FAILED (surfacing the error message and
                failing formats), or if RUNNING is not reached within timeout.
        """

        def raise_if_failed(candidate: TableflowTopic) -> None:
            if candidate.phase is TableflowPhase.FAILED:
                failing = "; ".join(
                    f"{f.format.value}: {f.error_message}"
                    for f in candidate.status.failing_table_formats
                )
                detail = candidate.status.error_message or ""
                raise OperationalError(
                    f"Tableflow for '{candidate.spec.display_name}' transitioned to FAILED: "
                    f"{detail} {failing}".strip()
                )

        raise_if_failed(topic)
        if topic.phase is TableflowPhase.RUNNING:
            return topic

        def poll(ctx: Context) -> TableflowTopic | None:
            nonlocal topic
            topic = self._get_topic(topic.spec.display_name, ctx)
            raise_if_failed(topic)
            return topic if topic.phase is TableflowPhase.RUNNING else None

        return poll_until(
            poll,
            timeout=timeout,
            immediate=False,
            on_timeout=lambda: OperationalError(
                f"Tableflow for '{topic.spec.display_name}' did not reach RUNNING within "
                f"{timeout} seconds"
            ),
        )

    def _wait_for_removal(self, table_name: str, timeout: float) -> None:
        """Poll `get` until it 404s, confirming the topic is gone.

        Raises:
            OperationalError: If the topic is not removed within timeout.
        """

        def is_removed(ctx: Context) -> bool | None:
            try:
                self._get_topic(table_name, ctx)
            except TableflowTopicNotFoundError:
                return True
            return None

        poll_until(
            is_removed,
            timeout=timeout,
            on_timeout=lambda: OperationalError(
                f"Tableflow for '{table_name}' was not removed within {timeout} seconds"
            ),
        )

    @staticmethod
    def _not_found(table_name: str) -> StatusExceptions:
        """The 404 -> `TableflowTopicNotFoundError` mapping shared by the calls that address an
        existing topic."""
        return {
            404: lambda _response: TableflowTopicNotFoundError(
                f"Tableflow is not enabled for table '{table_name}'", table_name=table_name
            )
        }
