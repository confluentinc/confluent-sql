"""
Tableflow types, request builders, and response parsers for the Confluent SQL DB-API driver.

These model the Tableflow Topic API (`/tableflow/v1/tableflow-topics`), which adds an Iceberg
or Delta materialization sink to the Kafka topic backing a Flink table. The request-side inputs
(storage specs, config, error-handling options) are frozen so a single value can be reused across
many `enable_tableflow` calls without risk of mutation. The response-side models mirror
`Statement`: raw `spec`/`status` retained, sub-structures parsed, a `.phase` convenience.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import ClassVar

from .exceptions import OperationalError
from .types import StrAnyDict


class TableFormat(str, Enum):
    """A concrete table format a Tableflow topic materializes to.

    API-faithful: this is what responses name (`spec.table_formats`,
    `status.failing_table_formats[].format`) and the unit of a future single-format disable.
    """

    ICEBERG = "ICEBERG"
    DELTA = "DELTA"


class TableFormatSelection(Enum):
    """The request-side pick of which format(s) to enable.

    Distinct from `TableFormat`: a topic can carry both formats at once, but there is no
    per-format config, so the choice is expressed as a single selection rather than a set. The
    `…Selection` suffix keeps it from being misread as `TableFormat` on a skim -- the
    `ICEBERG_AND_DELTA` member is not itself a "format".
    """

    ICEBERG = "ICEBERG"
    DELTA = "DELTA"
    ICEBERG_AND_DELTA = "ICEBERG_AND_DELTA"

    def to_wire(self) -> list[str]:
        """Expand the selection to the wire `spec.table_formats` array (minItems 1)."""
        if self is TableFormatSelection.ICEBERG_AND_DELTA:
            return [TableFormat.ICEBERG.value, TableFormat.DELTA.value]
        return [self.value]


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
        return {"kind": self.kind}


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
            "kind": self.kind,
            "bucket_name": self.bucket_name,
            "provider_integration_id": self.provider_integration_id,
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
            "kind": self.kind,
            "storage_account_name": self.storage_account_name,
            "container_name": self.container_name,
            "provider_integration_id": self.provider_integration_id,
        }


def storage_from_spec(data: StrAnyDict) -> TableflowStorage:
    """Parse a response `spec.storage` object into its typed storage class.

    Captures only the writable fields; server-assigned read-only fields (`table_path`,
    `bucket_region`, `storage_region`) remain available on the topic's raw spec dict.
    """
    kind = data.get("kind")
    if kind == ManagedStorage.kind:
        return ManagedStorage()
    if kind == ByobAwsStorage.kind:
        return ByobAwsStorage(
            bucket_name=data["bucket_name"],
            provider_integration_id=data["provider_integration_id"],
        )
    if kind == AzureAdlsStorage.kind:
        return AzureAdlsStorage(
            storage_account_name=data["storage_account_name"],
            container_name=data["container_name"],
            provider_integration_id=data["provider_integration_id"],
        )
    raise OperationalError(f"Wacky -- unknown Tableflow storage kind '{kind}' in response")


@dataclass(frozen=True)
class ErrorHandling:
    """Base for record-failure handling (`spec.config.error_handling`, oneOf on `mode`)."""

    mode: ClassVar[str]

    def to_spec(self) -> StrAnyDict:
        """Render to the wire `error_handling` object."""
        return {"mode": self.mode}


@dataclass(frozen=True)
class ErrorHandlingSuspend(ErrorHandling):
    """Suspend materialization on a bad record (the server default)."""

    mode: ClassVar[str] = "SUSPEND"


@dataclass(frozen=True)
class ErrorHandlingSkip(ErrorHandling):
    """Skip bad records and continue materializing."""

    mode: ClassVar[str] = "SKIP"


@dataclass(frozen=True)
class ErrorHandlingLog(ErrorHandling):
    """Log bad records to a dead-letter topic and continue materializing."""

    mode: ClassVar[str] = "LOG"

    target: str = "error_log"

    def to_spec(self) -> StrAnyDict:
        return {"mode": self.mode, "target": self.target}


@dataclass(frozen=True)
class TableflowTopicConfig:
    """Topic-level Tableflow config, shared across all enabled formats.

    Only the writable fields are modeled; `to_spec` emits only those actually set, so an empty
    config sends nothing. The deprecated `record_failure_strategy` and the read-only
    `enable_compaction`/`enable_partitioning` flags are deliberately omitted.
    """

    retention_ms: str | int | None = None
    data_retention_ms: str | int | None = None
    error_handling: ErrorHandling | None = None

    def to_spec(self) -> StrAnyDict:
        spec: StrAnyDict = {}
        if self.retention_ms is not None:
            spec["retention_ms"] = self.retention_ms
        if self.data_retention_ms is not None:
            spec["data_retention_ms"] = self.data_retention_ms
        if self.error_handling is not None:
            spec["error_handling"] = self.error_handling.to_spec()
        return spec


def build_create_payload(
    *,
    table_name: str,
    tableflow_format: TableFormatSelection,
    storage: TableflowStorage,
    config: TableflowTopicConfig | None,
    environment_id: str,
    kafka_cluster_id: str,
) -> StrAnyDict:
    """Assemble the `POST /tableflow/v1/tableflow-topics` request body.

    `table_name` is the Flink table, which is the backing Kafka topic name, which is
    `spec.display_name`. An empty config is omitted entirely.
    """
    spec: StrAnyDict = {
        "display_name": table_name,
        "storage": storage.to_spec(),
        "table_formats": tableflow_format.to_wire(),
        "environment": {"id": environment_id},
        "kafka_cluster": {"id": kafka_cluster_id},
    }
    if config is not None:
        config_spec = config.to_spec()
        if config_spec:
            spec["config"] = config_spec
    return {"spec": spec}


@dataclass
class FailingTableFormat:
    """A format that failed to materialize, with its error (`status.failing_table_formats`)."""

    format: TableFormat
    error_message: str

    @classmethod
    def from_response(cls, data: StrAnyDict) -> FailingTableFormat:
        return cls(format=TableFormat(data["format"]), error_message=data["error_message"])


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
    """Parsed topic spec; `table_formats` and `storage` are typed, `config` retained raw.

    The raw spec dict is kept (mirroring `Statement`). Config is left as a dict because its
    response carries read-only fields (`enable_compaction`, `enable_partitioning`) the writable
    `TableflowTopicConfig` doesn't model.
    """

    display_name: str
    table_formats: list[TableFormat]
    storage: TableflowStorage
    config: StrAnyDict | None
    environment_id: str | None
    kafka_cluster_id: str | None
    suspended: bool
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> TableflowTopicSpec:
        return cls(
            display_name=data["display_name"],
            table_formats=[TableFormat(fmt) for fmt in data.get("table_formats", [])],
            storage=storage_from_spec(data["storage"]),
            config=data.get("config"),
            environment_id=(data.get("environment") or {}).get("id"),
            kafka_cluster_id=(data.get("kafka_cluster") or {}).get("id"),
            suspended=bool(data.get("suspended", False)),
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
            raise OperationalError(
                f"Error parsing Tableflow topic response, missing {e}."
            ) from e
        return cls(spec=spec, status=status, metadata=metadata)
