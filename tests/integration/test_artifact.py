"""Integration test for the Flink artifact lifecycle against a live environment.

Needs a control-plane credential (a global API key) and the usual environment/cloud/region vars.
Uses a tiny throwaway ZIP: artifact create does not validate the archive's contents, and nothing
here registers a function from it (the artifact UDF example covers that).
"""

from __future__ import annotations

import contextlib
import io
import zipfile
from collections.abc import Generator
from datetime import datetime

import pytest

from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactNotFoundError,
    ArtifactRuntimeLanguage,
    Connection,
)

ORIGINAL_DESCRIPTION = "confluent-sql integration test artifact"
UPDATED_DESCRIPTION = "updated by confluent-sql integration test"


def _tiny_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("example/__init__.py", "")
    return buffer.getvalue()


@pytest.fixture()
def artifact_name(filtered_username: str) -> str:
    return f"confluentsql_pytest_{filtered_username}_{datetime.now():%Y%m%d_%H%M%S}"[:60]


@pytest.fixture()
def created_artifact_ids(connection: Connection) -> Generator[list[str], None, None]:
    """Collects ids of artifacts a test creates, deleting any still present on teardown."""
    ids: list[str] = []
    yield ids
    for artifact_id in ids:
        with contextlib.suppress(ArtifactNotFoundError):
            connection.delete_artifact(artifact_id)


@pytest.mark.integration
class TestArtifactLifecycle:
    def test_create_read_list_update_delete_arc(
        self, connection: Connection, artifact_name: str, created_artifact_ids: list[str]
    ) -> None:
        artifact = connection.create_artifact(
            artifact_name,
            file=_tiny_zip(),
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
            description=ORIGINAL_DESCRIPTION,
        )
        created_artifact_ids.append(artifact.id)
        assert artifact.display_name == artifact_name
        assert artifact.content_format is ArtifactContentFormat.ZIP

        read_back = connection.get_artifact(artifact.id)
        assert read_back.description == ORIGINAL_DESCRIPTION
        assert read_back.latest_version is not None

        assert artifact.id in {a.id for a in connection.list_artifacts()}
        assert artifact.id in {
            a.id for a in connection.list_artifacts(runtime_language=ArtifactRuntimeLanguage.PYTHON)
        }

        updated = connection.update_artifact(artifact.id, description=UPDATED_DESCRIPTION)
        assert updated.description == UPDATED_DESCRIPTION

        connection.delete_artifact(artifact.id)  # blocks until gone
        with pytest.raises(ArtifactNotFoundError):
            connection.get_artifact(artifact.id)

    def test_two_step_upload_via_presigned_url(
        self, connection: Connection, artifact_name: str, created_artifact_ids: list[str]
    ) -> None:
        target = connection.get_artifact_upload_url(ArtifactContentFormat.ZIP)
        connection.upload_artifact_file(target, _tiny_zip())
        artifact = connection.create_artifact(
            artifact_name,
            upload_id=target.upload_id,
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
        )
        created_artifact_ids.append(artifact.id)
        assert connection.get_artifact(artifact.id).display_name == artifact_name

    def test_duplicate_display_name_is_rejected(
        self, connection: Connection, artifact_name: str, created_artifact_ids: list[str]
    ) -> None:
        first = connection.create_artifact(
            artifact_name,
            file=_tiny_zip(),
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
        )
        created_artifact_ids.append(first.id)
        with pytest.raises(ArtifactAlreadyExistsError):
            connection.create_artifact(
                artifact_name,
                file=_tiny_zip(),
                content_format=ArtifactContentFormat.ZIP,
                runtime_language=ArtifactRuntimeLanguage.PYTHON,
            )

    def test_get_missing_artifact_raises_not_found(self, connection: Connection) -> None:
        with pytest.raises(ArtifactNotFoundError):
            connection.get_artifact("cfa-doesnotexist")
