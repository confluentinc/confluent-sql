"""Unit tests for the network-free Tableflow types, builders, and response parsers."""

from __future__ import annotations

import dataclasses

import pytest

from confluent_sql.exceptions import InterfaceError, OperationalError
from confluent_sql.tableflow import (
    AzureAdlsStorage,
    ByobAwsStorage,
    FailingTableFormat,
    ManagedStorage,
    TableflowErrorHandlingLog,
    TableflowErrorHandlingSkip,
    TableflowErrorHandlingSuspend,
    TableflowPhase,
    TableflowTopic,
    TableflowTopicConfig,
    TableFormat,
    build_create_payload,
    normalize_table_formats,
    storage_from_spec,
)

pytestmark = pytest.mark.unit


class TestNormalizeTableFormats:
    """The `tableflow_formats` argument accepts a single TableFormat or a collection of them."""

    def test_singleton(self) -> None:
        assert normalize_table_formats(TableFormat.ICEBERG) == ["ICEBERG"]

    def test_single_element_collection(self) -> None:
        assert normalize_table_formats([TableFormat.DELTA]) == ["DELTA"]

    def test_both_formats_canonically_ordered(self) -> None:
        # Declaration order (ICEBERG before DELTA) regardless of input order -- deterministic body.
        assert normalize_table_formats({TableFormat.DELTA, TableFormat.ICEBERG}) == [
            "ICEBERG",
            "DELTA",
        ]

    def test_duplicate_raises(self) -> None:
        with pytest.raises(InterfaceError, match="duplicate formats: ICEBERG"):
            normalize_table_formats([TableFormat.ICEBERG, TableFormat.ICEBERG])

    def test_empty_collection_raises(self) -> None:
        with pytest.raises(InterfaceError, match="at least one TableFormat"):
            normalize_table_formats([])

    def test_unknown_format_raises(self) -> None:
        with pytest.raises(InterfaceError, match="unknown table format"):
            normalize_table_formats(["ICEBURG"])  # type: ignore[list-item]


class TestTableflowPhase:
    """Phase is an extensible enum: known members plus a lenient unknown fallback."""

    @pytest.mark.parametrize(
        ("phase", "terminal"),
        [
            (TableflowPhase.PENDING, False),
            (TableflowPhase.RUNNING, True),
            (TableflowPhase.FAILED, True),
            (TableflowPhase.UNKNOWN, False),
        ],
    )
    def test_is_terminal(self, phase: TableflowPhase, terminal: bool) -> None:
        assert phase.is_terminal is terminal

    def test_unknown_value_maps_to_unknown(self) -> None:
        assert TableflowPhase("SOME_FUTURE_STATE") is TableflowPhase.UNKNOWN

    def test_missing_value_maps_to_unknown(self) -> None:
        assert TableflowPhase(None) is TableflowPhase.UNKNOWN


class TestStorageToSpec:
    """Each storage variant emits its `kind` discriminator plus only its writable fields."""

    def test_managed(self) -> None:
        assert ManagedStorage().to_spec() == {"kind": "Managed"}

    def test_byob_aws(self) -> None:
        storage = ByobAwsStorage(bucket_name="my-bucket", provider_integration_id="cspi-abc")
        assert storage.to_spec() == {
            "kind": "ByobAws",
            "bucket_name": "my-bucket",
            "provider_integration_id": "cspi-abc",
        }

    def test_azure_adls(self) -> None:
        storage = AzureAdlsStorage(
            storage_account_name="acct1",
            container_name="container1",
            provider_integration_id="cspi-xyz",
        )
        assert storage.to_spec() == {
            "kind": "AzureDataLakeStorageGen2",
            "storage_account_name": "acct1",
            "container_name": "container1",
            "provider_integration_id": "cspi-xyz",
        }

    def test_storage_is_frozen(self) -> None:
        storage = ByobAwsStorage(bucket_name="b", provider_integration_id="cspi-1")
        with pytest.raises(dataclasses.FrozenInstanceError):
            storage.bucket_name = "other"  # type: ignore[misc]


class TestStorageFromSpec:
    """Parsing the response `spec.storage` back into a typed object, by `kind`."""

    def test_managed(self) -> None:
        assert storage_from_spec({"kind": "Managed", "table_path": "s3://x"}) == ManagedStorage()

    def test_byob_aws_drops_readonly_fields(self) -> None:
        parsed = storage_from_spec(
            {
                "kind": "ByobAws",
                "bucket_name": "b",
                "provider_integration_id": "cspi-1",
                "bucket_region": "us-east-1",
                "table_path": "s3://x",
            }
        )
        assert parsed == ByobAwsStorage(bucket_name="b", provider_integration_id="cspi-1")

    def test_azure_adls_drops_readonly_fields(self) -> None:
        parsed = storage_from_spec(
            {
                "kind": "AzureDataLakeStorageGen2",
                "storage_account_name": "acct1",
                "container_name": "container1",
                "provider_integration_id": "cspi-xyz",
                "storage_region": "centralus",
                "table_path": "abfss://x",
            }
        )
        assert parsed == AzureAdlsStorage(
            storage_account_name="acct1",
            container_name="container1",
            provider_integration_id="cspi-xyz",
        )

    def test_unknown_kind_raises_wacky(self) -> None:
        with pytest.raises(OperationalError, match="Wacky -- .*storage kind 'Martian'"):
            storage_from_spec({"kind": "Martian"})


class TestTableflowErrorHandlingToSpec:
    """Error-handling option variants are discriminated on `mode`; only LOG carries a target."""

    def test_suspend(self) -> None:
        assert TableflowErrorHandlingSuspend().to_spec() == {"mode": "SUSPEND"}

    def test_skip(self) -> None:
        assert TableflowErrorHandlingSkip().to_spec() == {"mode": "SKIP"}

    def test_log_default_target(self) -> None:
        assert TableflowErrorHandlingLog().to_spec() == {"mode": "LOG", "target": "error_log"}

    def test_log_custom_target(self) -> None:
        assert TableflowErrorHandlingLog(target="my_dlq").to_spec() == {
            "mode": "LOG",
            "target": "my_dlq",
        }


class TestTableflowTopicConfig:
    """Config emits only the fields actually set; an empty config emits an empty dict."""

    def test_empty(self) -> None:
        assert TableflowTopicConfig().to_spec() == {}

    def test_retention_only(self) -> None:
        assert TableflowTopicConfig(retention_ms="604800000").to_spec() == {
            "retention_ms": "604800000"
        }

    def test_all_fields(self) -> None:
        config = TableflowTopicConfig(
            retention_ms="604800000",
            data_retention_ms="2592000000",
            error_handling=TableflowErrorHandlingLog(target="dlq"),
        )
        assert config.to_spec() == {
            "retention_ms": "604800000",
            "data_retention_ms": "2592000000",
            "error_handling": {"mode": "LOG", "target": "dlq"},
        }


class TestBuildCreatePayload:
    """The POST body assembles spec from the wire formats, storage, config, and connection ids."""

    def test_minimal(self) -> None:
        payload = build_create_payload(
            table_name="orders",
            table_formats=["ICEBERG"],
            storage=ManagedStorage(),
            config=None,
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
        )
        assert payload == {
            "spec": {
                "display_name": "orders",
                "storage": {"kind": "Managed"},
                "table_formats": ["ICEBERG"],
                "environment": {"id": "env-1"},
                "kafka_cluster": {"id": "lkc-1"},
            }
        }

    def test_both_formats_and_config(self) -> None:
        payload = build_create_payload(
            table_name="orders",
            table_formats=["ICEBERG", "DELTA"],
            storage=ManagedStorage(),
            config=TableflowTopicConfig(retention_ms="604800000"),
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
        )
        assert payload["spec"]["table_formats"] == ["ICEBERG", "DELTA"]
        assert payload["spec"]["config"] == {"retention_ms": "604800000"}

    def test_empty_config_omitted(self) -> None:
        payload = build_create_payload(
            table_name="orders",
            table_formats=["DELTA"],
            storage=ManagedStorage(),
            config=TableflowTopicConfig(),
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
        )
        assert "config" not in payload["spec"]


def _topic_response(
    *,
    display_name: str = "orders",
    phase: str = "PENDING",
    table_formats: list[str] | None = None,
    failing_table_formats: list[dict] | None = None,
) -> dict:
    """A tableflow.v1.TableflowTopic JSON dict, mirroring the create/read response shape."""
    return {
        "api_version": "tableflow/v1",
        "kind": "TableflowTopic",
        "metadata": {"self": "https://api.confluent.cloud/tableflow/v1/tableflow-topics/orders"},
        "spec": {
            "display_name": display_name,
            "storage": {"kind": "Managed", "table_path": "s3://x"},
            "table_formats": table_formats if table_formats is not None else ["ICEBERG"],
            "environment": {"id": "env-1"},
            "kafka_cluster": {"id": "lkc-1"},
            "suspended": False,
            "config": {"retention_ms": "604800000", "enable_compaction": True},
        },
        "status": {
            "phase": phase,
            "write_mode": "APPEND",
            "error_message": "",
            "failing_table_formats": failing_table_formats or [],
        },
    }


class TestTableflowTopicFromResponse:
    """Parsing the full response into the read model and its sub-structures."""

    def test_parses_spec_and_status(self) -> None:
        topic = TableflowTopic.from_response(
            _topic_response(table_formats=["ICEBERG", "DELTA"], phase="RUNNING")
        )
        assert topic.spec.display_name == "orders"
        assert topic.spec.table_formats == [TableFormat.ICEBERG, TableFormat.DELTA]
        assert topic.spec.storage == ManagedStorage()
        assert topic.spec.environment_id == "env-1"
        assert topic.spec.kafka_cluster_id == "lkc-1"
        assert topic.spec.suspended is False
        assert topic.spec.config == {"retention_ms": "604800000", "enable_compaction": True}
        assert topic.status.write_mode == "APPEND"
        assert topic.phase is TableflowPhase.RUNNING
        assert topic.status.phase is TableflowPhase.RUNNING

    def test_parses_failing_table_formats(self) -> None:
        topic = TableflowTopic.from_response(
            _topic_response(
                phase="FAILED",
                failing_table_formats=[
                    {"format": "ICEBERG", "error_message": "Schema validation failed"}
                ],
            )
        )
        assert topic.status.failing_table_formats == [
            FailingTableFormat(format=TableFormat.ICEBERG, error_message="Schema validation failed")
        ]

    def test_unknown_phase_is_lenient(self) -> None:
        topic = TableflowTopic.from_response(_topic_response(phase="SOME_FUTURE_STATE"))
        assert topic.phase is TableflowPhase.UNKNOWN

    def test_missing_required_section_raises(self) -> None:
        response = _topic_response()
        del response["status"]
        with pytest.raises(OperationalError, match="missing 'status'"):
            TableflowTopic.from_response(response)
