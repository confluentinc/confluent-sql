"""Integration test for the Flink artifact lifecycle against a live environment.

Needs a global API key, or a Cloud resource management key (CONFLUENT_CLOUD_API_KEY/SECRET) next to
a Flink key, and the usual environment/cloud/region vars; skips otherwise.
Uses a tiny throwaway ZIP: artifact create does not validate the archive's contents, and nothing
here registers a function from it (the artifact UDF example covers that).
"""

from __future__ import annotations

import contextlib
import io
import os
import zipfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import pytest

import confluent_sql
from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactNotFoundError,
    ArtifactRuntimeLanguage,
    ArtifactsApi,
    Connection,
)

ORIGINAL_DESCRIPTION = "confluent-sql integration test artifact"


def _tiny_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("example/__init__.py", "")
    return buffer.getvalue()


def _artifact_connection_from_env() -> Connection:
    """Build an artifact-capable Connection from env, or skip if the configuration is incomplete.

    Artifact routes need a global key or a Cloud resource management pair; the connection's own
    Flink client needs a global or Flink-region pair.
    """
    global_api_key = os.getenv("CONFLUENT_GLOBAL_API_KEY", "")
    global_api_secret = os.getenv("CONFLUENT_GLOBAL_API_SECRET", "")
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY", "")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET", "")
    cloud_api_key = os.getenv("CONFLUENT_CLOUD_API_KEY", "")
    cloud_api_secret = os.getenv("CONFLUENT_CLOUD_API_SECRET", "")
    environment_id = os.getenv("CONFLUENT_ENV_ID", "")
    organization_id = os.getenv("CONFLUENT_ORG_ID", "")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "")
    database = os.getenv("CONFLUENT_TEST_DBNAME", "")

    has_global = bool(global_api_key) and bool(global_api_secret)
    has_flink = bool(flink_api_key) and bool(flink_api_secret)
    has_cloud = bool(cloud_api_key) and bool(cloud_api_secret)
    # A half-supplied pair would make connect() raise; treat it as "not configured" and skip.
    no_half_pairs = not (
        bool(global_api_key) != bool(global_api_secret)
        or bool(flink_api_key) != bool(flink_api_secret)
        or bool(cloud_api_key) != bool(cloud_api_secret)
    )
    # organization_id is inferable from a global key (#132), so it's only required outright when
    # no global key is configured.
    org_ok = has_global or bool(organization_id)
    base_ok = all([environment_id, org_ok, cloud_provider, cloud_region, database])
    if not (no_half_pairs and (has_global or has_flink) and (has_global or has_cloud) and base_ok):
        pytest.skip("Missing environment variables for an artifact-capable integration connection")

    return confluent_sql.connect(
        global_api_key=global_api_key,
        global_api_secret=global_api_secret,
        flink_api_key=flink_api_key,
        flink_api_secret=flink_api_secret,
        cloud_api_key=cloud_api_key,
        cloud_api_secret=cloud_api_secret,
        environment_id=environment_id,
        organization_id=organization_id,
        cloud_provider=cloud_provider,
        cloud_region=cloud_region,
        database=database,
    )


@pytest.fixture()
def artifact_connection(load_env_file) -> Generator[Connection, None, None]:
    """An artifact-capable connection, closed at the end of the test."""
    conn = _artifact_connection_from_env()
    yield conn
    conn.close()


@pytest.fixture()
def artifacts(artifact_connection: Connection) -> ArtifactsApi:
    return artifact_connection.get_artifacts_api()


@pytest.fixture()
def artifact_name(filtered_username: str) -> str:
    return f"confluentsql_pytest_{filtered_username}_{datetime.now():%Y%m%d_%H%M%S}"[:60]


@pytest.fixture()
def created_artifact_ids(artifacts: ArtifactsApi) -> Generator[list[str], None, None]:
    """Collects ids of artifacts a test creates, deleting any still present on teardown."""
    ids: list[str] = []
    yield ids
    for artifact_id in ids:
        with contextlib.suppress(ArtifactNotFoundError):
            artifacts.delete(artifact_id)


@pytest.fixture()
def zip_on_disk(tmp_path: Path) -> Path:
    path = tmp_path / "udf_package.zip"
    path.write_bytes(_tiny_zip())
    return path


@pytest.mark.integration
class TestArtifactLifecycle:
    def test_create_read_list_delete_arc(
        self,
        artifacts: ArtifactsApi,
        artifact_name: str,
        created_artifact_ids: list[str],
        zip_on_disk: Path,
    ) -> None:
        artifact = artifacts.create(
            artifact_name,
            file=zip_on_disk,
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
            description=ORIGINAL_DESCRIPTION,
        )
        created_artifact_ids.append(artifact.id)
        assert artifact.display_name == artifact_name
        assert artifact.content_format is ArtifactContentFormat.ZIP

        read_back = artifacts.get(artifact.id)
        assert read_back.description == ORIGINAL_DESCRIPTION
        assert read_back.latest_version is not None

        assert artifact.id in {a.id for a in artifacts.list()}
        assert artifact.id in {
            a.id for a in artifacts.list(runtime_language=ArtifactRuntimeLanguage.PYTHON)
        }

        artifacts.delete(artifact.id)  # blocks until gone
        with pytest.raises(ArtifactNotFoundError):
            artifacts.get(artifact.id)

    def test_duplicate_display_name_is_rejected(
        self,
        artifacts: ArtifactsApi,
        artifact_name: str,
        created_artifact_ids: list[str],
        zip_on_disk: Path,
    ) -> None:
        first = artifacts.create(
            artifact_name,
            file=zip_on_disk,
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
        )
        created_artifact_ids.append(first.id)
        with pytest.raises(ArtifactAlreadyExistsError):
            artifacts.create(
                artifact_name,
                file=zip_on_disk,
                content_format=ArtifactContentFormat.ZIP,
                runtime_language=ArtifactRuntimeLanguage.PYTHON,
            )

    def test_get_missing_artifact_raises_not_found(self, artifacts: ArtifactsApi) -> None:
        with pytest.raises(ArtifactNotFoundError):
            artifacts.get("cfa-doesnotexist")


@pytest.mark.integration
class TestArtifactFileInputs:
    """Inferring `content_format` from the archive's extension."""

    def test_create_from_path_infers_content_format(
        self,
        artifacts: ArtifactsApi,
        artifact_name: str,
        created_artifact_ids: list[str],
        zip_on_disk: Path,
    ) -> None:
        # No content_format: it must be inferred from the ".zip" suffix.
        artifact = artifacts.create(
            artifact_name, file=zip_on_disk, runtime_language=ArtifactRuntimeLanguage.PYTHON
        )
        created_artifact_ids.append(artifact.id)

        assert artifact.content_format is ArtifactContentFormat.ZIP
        assert artifacts.get(artifact.id).latest_version is not None
