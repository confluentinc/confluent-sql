"""Unit tests for Flink artifact models, request builders, and the ArtifactsApi orchestration."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import httpx
import pytest

from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactNotFoundError,
    ArtifactRuntimeLanguage,
    FlinkArtifact,
    InterfaceError,
    OperationalError,
    UploadRetry,
)
from confluent_sql.artifacts import (
    ArtifactsApi,
    ArtifactScope,
    PresignedUploadUrl,
    build_create_payload,
    build_presign_payload,
    infer_content_format,
)

pytestmark = pytest.mark.unit

ENV_ID = "env-1"
CLOUD = "AWS"
REGION = "us-east-2"
ARTIFACT_ID = "cfa-abc123"
UPLOAD_ID = "upload-1"
UPLOAD_URL = "https://bucket.s3.example.com/"
FORM_DATA = {"key": "k", "policy": "p", "x-amz-signature": "sig"}


def _artifact_body(**overrides: Any) -> dict:
    body = {
        "api_version": "artifact/v1",
        "kind": "FlinkArtifact",
        "id": ARTIFACT_ID,
        "display_name": "my-udf",
        "cloud": CLOUD,
        "region": REGION,
        "environment": ENV_ID,
        "content_format": "ZIP",
        "runtime_language": "PYTHON",
        "description": "d",
        "documentation_link": "",
        "class": "default",
        "versions": [{"version": "ver-1", "release_notes": "", "upload_source": {}}],
    }
    body.update(overrides)
    return body


def _presign_body() -> dict:
    return {
        "upload_id": UPLOAD_ID,
        "upload_url": UPLOAD_URL,
        "upload_form_data": FORM_DATA,
        "content_format": "ZIP",
        "cloud": CLOUD,
        "region": REGION,
    }


def _response(body: dict | None = None, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body if body is not None else {})


class FakeContext:
    """A stand-in for Connection: records requests and serves queued responses."""

    environment_id = ENV_ID

    def __init__(self, *responses: httpx.Response) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, str, dict]] = []

    def artifact_scope(self) -> ArtifactScope:
        return ArtifactScope(CLOUD, REGION)

    def artifact_controlplane_request(
        self, url: str, method: str = "GET", raise_for_status: bool = True, **kwargs: Any
    ) -> httpx.Response:
        # Snapshot: the real code reuses (and mutates) one params dict across pages.
        self.requests.append((method, url, copy.deepcopy(kwargs)))
        return self.responses.pop(0)


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    path = tmp_path / "udf.zip"
    path.write_bytes(b"payload")
    return path


@pytest.fixture
def upload_transport(mocker) -> list[httpx.Request]:
    """Route the credential-free upload client through a MockTransport; returns the requests it
    saw. The mock replies 204 (what S3 returns for a successful form POST)."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    real_client = httpx.Client
    mocker.patch(
        "confluent_sql.artifacts.httpx.Client",
        side_effect=lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
    )
    return seen


class TestModels:
    def test_presigned_upload_url_repr_hides_signed_capability(self) -> None:
        target = PresignedUploadUrl.from_response(_presign_body())
        shown = repr(target)
        assert UPLOAD_URL not in shown
        assert not any(field_name in shown for field_name in FORM_DATA)
        assert UPLOAD_ID in shown

    def test_artifact_from_response(self) -> None:
        artifact = FlinkArtifact.from_response(_artifact_body())
        assert artifact.id == ARTIFACT_ID
        assert artifact.content_format is ArtifactContentFormat.ZIP
        assert artifact.runtime_language is ArtifactRuntimeLanguage.PYTHON
        assert artifact.class_name == "default"
        assert [v.version for v in artifact.versions] == ["ver-1"]
        assert artifact.latest_version is artifact.versions[0]

    def test_artifact_without_versions_has_no_latest(self) -> None:
        artifact = FlinkArtifact.from_response(_artifact_body(versions=None))
        assert artifact.versions == []
        assert artifact.latest_version is None

    def test_unrecognized_enum_values_parse_to_unknown(self) -> None:
        artifact = FlinkArtifact.from_response(
            _artifact_body(content_format="TARBALL", runtime_language="RUST")
        )
        assert artifact.content_format is ArtifactContentFormat.UNKNOWN
        assert artifact.runtime_language is ArtifactRuntimeLanguage.UNKNOWN

    def test_missing_required_field_is_operational_error(self) -> None:
        body = _artifact_body()
        del body["id"]
        with pytest.raises(OperationalError, match="missing 'id'"):
            FlinkArtifact.from_response(body)

    def test_presigned_url_tolerates_missing_echo_fields(self) -> None:
        body = _presign_body()
        del body["cloud"], body["content_format"]
        target = PresignedUploadUrl.from_response(body)
        assert (target.upload_id, target.upload_url) == (UPLOAD_ID, UPLOAD_URL)
        assert target.upload_form_data == FORM_DATA
        assert target.cloud is None and target.content_format is None

    def test_presigned_url_missing_upload_id_is_operational_error(self) -> None:
        body = _presign_body()
        del body["upload_id"]
        with pytest.raises(OperationalError, match="missing 'upload_id'"):
            PresignedUploadUrl.from_response(body)


class TestPayloads:
    def test_presign_payload(self) -> None:
        assert build_presign_payload(
            content_format=ArtifactContentFormat.JAR, cloud=CLOUD, region=REGION, environment=ENV_ID
        ) == {
            "content_format": "JAR",
            "cloud": CLOUD,
            "region": REGION,
            "environment": ENV_ID,
        }

    def test_create_payload_minimal_omits_unset_optionals(self) -> None:
        payload = build_create_payload(
            display_name="n", upload_id=UPLOAD_ID, cloud=CLOUD, region=REGION, environment=ENV_ID
        )
        assert payload == {
            "cloud": CLOUD,
            "region": REGION,
            "environment": ENV_ID,
            "display_name": "n",
            "upload_source": {"location": "PRESIGNED_URL_LOCATION", "upload_id": UPLOAD_ID},
        }

    def test_create_payload_with_optionals(self) -> None:
        payload = build_create_payload(
            display_name="n",
            upload_id=UPLOAD_ID,
            cloud=CLOUD,
            region=REGION,
            environment=ENV_ID,
            content_format=ArtifactContentFormat.ZIP,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
            description="desc",
            documentation_link="https://x",
            class_name="a.B",
        )
        assert payload["content_format"] == "ZIP"
        assert payload["runtime_language"] == "PYTHON"
        assert payload["description"] == "desc"
        assert payload["documentation_link"] == "https://x"
        assert payload["class"] == "a.B"

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("a.jar", ArtifactContentFormat.JAR),
            (Path("dir/B.ZIP"), ArtifactContentFormat.ZIP),
            ("a.tar.gz", None),
        ],
    )
    def test_infer_content_format(self, path: Any, expected: ArtifactContentFormat | None) -> None:
        assert infer_content_format(path) is expected


class TestUpload:
    def test_posts_form_fields_then_file_without_credentials(
        self, upload_transport: list[httpx.Request], tmp_path: Path
    ) -> None:
        archive = tmp_path / "udf.zip"
        archive.write_bytes(b"zipbytes")
        target = PresignedUploadUrl.from_response(_presign_body())

        ArtifactsApi(FakeContext())._upload(target, archive)

        (request,) = upload_transport
        assert request.method == "POST"
        assert str(request.url) == UPLOAD_URL
        assert "authorization" not in request.headers
        body = request.read()
        assert b'filename="udf.zip"' in body and b"zipbytes" in body
        # S3 requires every form field to precede the file part.
        assert body.index(b'name="policy"') < body.index(b'name="file"')

    def test_upload_timeout_is_applied_to_the_upload_client(self, mocker, archive: Path) -> None:
        client_cls = mocker.patch("confluent_sql.artifacts.httpx.Client")
        client_cls.return_value.__enter__.return_value.post.return_value = httpx.Response(204)
        target = PresignedUploadUrl.from_response(_presign_body())
        ArtifactsApi(FakeContext())._upload(target, archive, upload_timeout=42)
        client_cls.assert_called_once_with(timeout=42)

    def test_unreadable_path_is_interface_error(self, tmp_path: Path) -> None:
        target = PresignedUploadUrl.from_response(_presign_body())
        with pytest.raises(InterfaceError, match="can't read artifact file"):
            ArtifactsApi(FakeContext())._upload(target, tmp_path / "missing.zip")

    def test_rejected_upload_is_operational_error_with_status(self, mocker, archive: Path) -> None:
        real_client = httpx.Client
        mocker.patch(
            "confluent_sql.artifacts.httpx.Client",
            side_effect=lambda **kw: real_client(
                transport=httpx.MockTransport(lambda _: httpx.Response(403, text="AccessDenied")),
                **kw,
            ),
        )
        target = PresignedUploadUrl.from_response(_presign_body())
        with pytest.raises(OperationalError, match="AccessDenied") as exc:
            ArtifactsApi(FakeContext())._upload(target, archive)
        assert exc.value.http_status_code == 403

    def test_transport_failure_is_operational_error(self, mocker, archive: Path) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")

        def boom(_: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("reset")

        real_client = httpx.Client
        mocker.patch(
            "confluent_sql.artifacts.httpx.Client",
            side_effect=lambda **kw: real_client(transport=httpx.MockTransport(boom), **kw),
        )
        target = PresignedUploadUrl.from_response(_presign_body())
        with pytest.raises(OperationalError, match="ConnectError"):
            ArtifactsApi(FakeContext())._upload(target, archive)


class TestUploadRetryPolicy:
    @pytest.mark.parametrize("field", ["connect_retries", "transfer_retries"])
    @pytest.mark.parametrize("bad", [-1, True, 1.5, float("inf"), float("nan"), "3"])
    def test_non_integer_or_negative_counts_are_rejected(self, field: str, bad: object) -> None:
        with pytest.raises(InterfaceError, match=f"{field} must be a non-negative integer"):
            UploadRetry(**{field: bad})  # type: ignore[arg-type]


class TestUploadRetry:
    """Connect failures (nothing sent) retry by default; mid-transfer failures re-send the whole
    archive, so they retry only when asked."""

    @staticmethod
    def _flaky_transport(mocker, failures: list[Exception]) -> list[bytes]:
        """Fail with each queued exception in turn, then succeed; returns the bodies received."""
        mocker.patch("confluent_sql.retry.time.sleep")
        bodies: list[bytes] = []

        def handler(request: httpx.Request) -> httpx.Response:
            bodies.append(request.read())
            if failures:
                raise failures.pop(0)
            return httpx.Response(204)

        real_client = httpx.Client
        mocker.patch(
            "confluent_sql.artifacts.httpx.Client",
            side_effect=lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
        )
        return bodies

    @staticmethod
    def _upload(archive: Path, **kwargs: Any) -> None:
        target = PresignedUploadUrl.from_response(_presign_body())
        ArtifactsApi(FakeContext())._upload(target, archive, **kwargs)

    def test_connect_errors_are_retried_by_default(self, mocker, archive: Path) -> None:
        bodies = self._flaky_transport(mocker, [httpx.ConnectError("ssl eof")] * 3)
        self._upload(archive)
        assert len(bodies) == 4

    def test_connect_retries_are_bounded_and_configurable(self, mocker, archive: Path) -> None:
        bodies = self._flaky_transport(mocker, [httpx.ConnectError("ssl eof")] * 5)
        with pytest.raises(OperationalError, match="ConnectError"):
            self._upload(archive, retry=UploadRetry(connect_retries=1))
        assert len(bodies) == 2

    def test_mid_transfer_errors_are_not_retried_by_default(self, mocker, archive: Path) -> None:
        bodies = self._flaky_transport(mocker, [httpx.WriteError("broken pipe")])
        with pytest.raises(OperationalError, match="WriteError"):
            self._upload(archive)
        assert len(bodies) == 1

    def test_mid_transfer_retry_resends_the_whole_file(self, mocker, archive: Path) -> None:
        bodies = self._flaky_transport(mocker, [httpx.WriteError("broken pipe")])
        self._upload(archive, retry=UploadRetry(transfer_retries=1))
        assert len(bodies) == 2
        assert all(b"payload" in body for body in bodies)

    def test_http_rejection_is_not_retried(self, mocker, archive: Path) -> None:
        mocker.patch("confluent_sql.retry.time.sleep")
        calls: list[int] = []

        def handler(_: httpx.Request) -> httpx.Response:
            calls.append(1)
            return httpx.Response(403, text="AccessDenied")

        real_client = httpx.Client
        mocker.patch(
            "confluent_sql.artifacts.httpx.Client",
            side_effect=lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
        )
        with pytest.raises(OperationalError, match="AccessDenied"):
            self._upload(archive, retry=UploadRetry(connect_retries=3, transfer_retries=3))
        assert len(calls) == 1


REPEATED_TOKEN = "tok-a"


class TestCreate:
    @pytest.mark.parametrize("field", ["content_format", "runtime_language"])
    def test_empty_enum_string_is_rejected_not_treated_as_unset(self, field: str) -> None:
        ctx = FakeContext()
        content_format = "" if field == "content_format" else None
        runtime_language = "" if field == "runtime_language" else None
        with pytest.raises(InterfaceError, match=f"unknown {field}"):
            ArtifactsApi(ctx).create(
                "my-udf",
                "udf.zip",
                content_format=content_format,
                runtime_language=runtime_language,
            )
        assert ctx.requests == []

    def test_file_flow_presigns_uploads_then_creates(
        self, upload_transport: list[httpx.Request], archive: Path
    ) -> None:
        ctx = FakeContext(_response(_presign_body()), _response(_artifact_body(), 201))

        artifact = ArtifactsApi(ctx).create(
            "my-udf", file=archive, runtime_language=ArtifactRuntimeLanguage.PYTHON
        )

        assert artifact.id == ARTIFACT_ID
        presign, create = ctx.requests
        assert presign[:2] == ("POST", "/artifact/v1/presigned-upload-url")
        assert presign[2]["json"]["content_format"] == "ZIP"  # inferred from the extension
        assert len(upload_transport) == 1
        assert create[:2] == ("POST", "/artifact/v1/flink-artifacts")
        assert create[2]["params"] == {"cloud": CLOUD, "region": REGION}
        assert create[2]["json"]["upload_source"]["upload_id"] == UPLOAD_ID
        assert create[2]["json"]["runtime_language"] == "PYTHON"

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"file": "a.tar.gz"}, "content_format is required"),
            ({"file": "a.zip", "content_format": "TARBALL"}, "unknown content_format"),
            (
                {"file": "a.jar", "runtime_language": "RUST"},
                "unknown runtime_language",
            ),
        ],
    )
    def test_argument_validation_fails_before_any_request(self, kwargs: dict, match: str) -> None:
        ctx = FakeContext()
        with pytest.raises(InterfaceError, match=match):
            ArtifactsApi(ctx).create("n", **kwargs)
        assert ctx.requests == []

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"file": "a.zip", "runtime_language": "JAVA"}, "JAVA artifacts must be JAR"),
            ({"file": "a.jar", "runtime_language": "PYTHON"}, "PYTHON artifacts must be ZIP"),
            (
                {"file": "a.tar.gz", "content_format": "JAR", "runtime_language": "PYTHON"},
                "PYTHON artifacts must be ZIP",
            ),
            # runtime_language defaults to JAVA server-side, so a bare ZIP is a mismatch.
            ({"file": "a.zip"}, "defaults to JAVA; pass runtime_language=PYTHON"),
        ],
    )
    def test_format_language_mismatch_fails_before_any_request(
        self, kwargs: dict, match: str, upload_transport
    ) -> None:
        ctx = FakeContext()
        with pytest.raises(InterfaceError, match=match):
            ArtifactsApi(ctx).create("n", **kwargs)
        assert ctx.requests == []
        assert upload_transport == []

    def test_explicit_content_format_overrides_the_extension(
        self, upload_transport, archive: Path
    ) -> None:
        ctx = FakeContext(_response(_presign_body()), _response(_artifact_body(), 201))
        ArtifactsApi(ctx).create("n", file=archive, content_format="JAR")
        assert ctx.requests[0][2]["json"]["content_format"] == "JAR"

    @pytest.mark.parametrize(
        "duplicate_response",
        [
            _response(status=409),
            # What the live API actually sends for a taken name.
            httpx.Response(
                400,
                json={"errors": [{"detail": "name should be unique per Cloud/Region/Environment"}]},
            ),
        ],
        ids=["409", "400-name-should-be-unique"],
    )
    def test_duplicate_name_is_already_exists_error(
        self, duplicate_response: httpx.Response, upload_transport, archive: Path
    ) -> None:
        ctx = FakeContext(_response(_presign_body()), duplicate_response)
        with pytest.raises(ArtifactAlreadyExistsError) as exc:
            ArtifactsApi(ctx).create("dup", archive, runtime_language="PYTHON")
        assert exc.value.display_name == "dup"
        assert exc.value.http_status_code == duplicate_response.status_code

    @pytest.mark.parametrize("status", [400, 422])
    def test_other_error_is_operational_error(
        self, status: int, upload_transport, archive: Path
    ) -> None:
        ctx = FakeContext(_response(_presign_body()), _response(status=status))
        with pytest.raises(OperationalError) as exc:
            ArtifactsApi(ctx).create("n", archive, runtime_language="PYTHON")
        assert exc.value.http_status_code == status

    @pytest.mark.parametrize("bad_name", ["", "   ", "x" * 61])
    def test_invalid_display_name_fails_before_presign_or_upload(
        self, bad_name: str, upload_transport, archive: Path
    ) -> None:
        ctx = FakeContext()
        with pytest.raises(InterfaceError, match="display_name"):
            ArtifactsApi(ctx).create(bad_name, file=archive)
        assert ctx.requests == []
        assert upload_transport == []


class TestMalformedResponses:
    HTML_PAGE = httpx.Response(200, text="<html>proxy error</html>")

    @pytest.mark.parametrize("form_data", [None, "not-a-dict", 5])
    def test_presigned_url_with_wrong_typed_form_data_is_operational_error(
        self, form_data: Any
    ) -> None:
        body = {**_presign_body(), "upload_form_data": form_data}
        with pytest.raises(OperationalError, match="Error parsing presigned upload URL"):
            PresignedUploadUrl.from_response(body)

    @pytest.mark.parametrize("versions", ["nope", 5, [None]])
    def test_artifact_with_wrong_typed_versions_is_operational_error(self, versions: Any) -> None:
        with pytest.raises(OperationalError, match="Error parsing artifact"):
            FlinkArtifact.from_response(_artifact_body(versions=versions))

    @pytest.mark.parametrize(
        "operation",
        [
            lambda api: api.get(ARTIFACT_ID),
            lambda api: api.list(),
            lambda api: api._get_upload_url("JAR"),
        ],
        ids=["get", "list", "presign"],
    )
    @pytest.mark.parametrize(
        "bad_response",
        [HTML_PAGE, httpx.Response(200, json=["not", "an", "object"])],
        ids=["non-json", "non-object"],
    )
    def test_unparseable_success_body_is_operational_error(
        self, operation: Any, bad_response: httpx.Response
    ) -> None:
        with pytest.raises(OperationalError, match="Error parsing") as exc:
            operation(ArtifactsApi(FakeContext(bad_response)))
        assert exc.value.http_status_code == 200

    @pytest.mark.parametrize(
        "body",
        [{"data": "x"}, {"metadata": "x"}, {"data": {"a": 1}}],
    )
    def test_list_with_malformed_data_or_metadata_is_operational_error(self, body: dict) -> None:
        api = ArtifactsApi(FakeContext(_response(body)))
        with pytest.raises(OperationalError, match="Error parsing artifact list"):
            api.list()

    @pytest.mark.parametrize(
        "body",
        [{}, {"data": None}, {"metadata": None}, {"data": None, "metadata": None}],
        ids=["absent", "null-data", "null-metadata", "both-null"],
    )
    def test_list_tolerates_absent_or_null_data_and_metadata(self, body: dict) -> None:
        """An empty collection arrives as an absent key or an explicit JSON null (#263)."""
        assert ArtifactsApi(FakeContext(_response(body))).list() == []


class TestGetListUpdateDelete:
    def test_get_scopes_by_cloud_region_environment(self) -> None:
        ctx = FakeContext(_response(_artifact_body()))
        artifact = ArtifactsApi(ctx).get(ARTIFACT_ID)
        assert artifact.display_name == "my-udf"
        assert ctx.requests == [
            (
                "GET",
                f"/artifact/v1/flink-artifacts/{ARTIFACT_ID}",
                {
                    "params": {"cloud": CLOUD, "region": REGION, "environment": ENV_ID},
                },
            )
        ]

    @pytest.mark.parametrize("operation", ["get", "delete"])
    @pytest.mark.parametrize("bad_id", ["", ".", ".."])
    def test_unusable_artifact_id_makes_no_request(self, operation: str, bad_id: str) -> None:
        ctx = FakeContext()
        api = ArtifactsApi(ctx)
        call = {
            "get": lambda: api.get(bad_id),
            "delete": lambda: api.delete(bad_id),
        }[operation]
        with pytest.raises(InterfaceError, match="invalid artifact_id"):
            call()
        assert ctx.requests == []

    @pytest.mark.parametrize("operation", ["get", "delete"])
    def test_artifact_id_is_percent_encoded_in_the_path(self, operation: str) -> None:
        hostile_id = "../x?y#z"
        ctx = FakeContext(_response(_artifact_body()))
        api = ArtifactsApi(ctx)
        {
            "get": lambda: api.get(hostile_id),
            "delete": lambda: api.delete(hostile_id, wait_for_removal=False),
        }[operation]()
        assert ctx.requests[0][1] == "/artifact/v1/flink-artifacts/..%2Fx%3Fy%23z"

    @pytest.mark.parametrize("operation", ["get", "delete"])
    def test_404_is_artifact_not_found(self, operation: str) -> None:
        api = ArtifactsApi(FakeContext(_response(status=404)))
        call = {
            "get": lambda: api.get(ARTIFACT_ID),
            "delete": lambda: api.delete(ARTIFACT_ID),
        }[operation]
        with pytest.raises(ArtifactNotFoundError) as exc:
            call()
        assert exc.value.artifact_id == ARTIFACT_ID
        assert exc.value.http_status_code == 404

    def test_list_follows_pagination_and_filters(self) -> None:
        page1 = {
            "data": [_artifact_body(id="cfa-1")],
            "metadata": {"next": "https://api/x?page_token=tok2"},
        }
        page2 = {"data": [_artifact_body(id="cfa-2")], "metadata": {}}
        ctx = FakeContext(_response(page1), _response(page2))

        artifacts = ArtifactsApi(ctx).list(runtime_language="PYTHON", page_size=1)

        assert [a.id for a in artifacts] == ["cfa-1", "cfa-2"]
        first, second = (r[2]["params"] for r in ctx.requests)
        assert first["runtime_language"] == "PYTHON" and first["page_size"] == "1"
        assert "page_token" not in first
        assert second["page_token"] == "tok2"

    def test_list_stops_on_empty_page_token(self) -> None:
        page = {
            "data": [_artifact_body(id="cfa-1")],
            "metadata": {"next": "https://api/x?page_token="},
        }
        ctx = FakeContext(_response(page))

        assert [a.id for a in ArtifactsApi(ctx).list()] == ["cfa-1"]
        assert len(ctx.requests) == 1

    def test_list_raises_on_repeated_page_token(self) -> None:
        # The second page hands back the same token that led to it.
        pages = [
            _response(
                {
                    "data": [_artifact_body(id=f"cfa-{i}")],
                    "metadata": {"next": f"https://api/x?page_token={REPEATED_TOKEN}"},
                }
            )
            for i in range(2)
        ]
        ctx = FakeContext(*pages)

        with pytest.raises(OperationalError, match=f"repeated page token .{REPEATED_TOKEN}."):
            ArtifactsApi(ctx).list()
        assert len(ctx.requests) == 2

    def test_delete_without_wait_returns_after_delete(self) -> None:
        ctx = FakeContext(_response(status=204))
        ArtifactsApi(ctx).delete(ARTIFACT_ID, wait_for_removal=False)
        assert [r[0] for r in ctx.requests] == ["DELETE"]

    def test_delete_waits_until_read_404s(self, mocker) -> None:
        mocker.patch("confluent_sql.artifacts.sleep_with_backoff", return_value=iter([None] * 3))
        ctx = FakeContext(_response(status=204), _response(_artifact_body()), _response(status=404))
        ArtifactsApi(ctx).delete(ARTIFACT_ID)
        assert [r[0] for r in ctx.requests] == ["DELETE", "GET", "GET"]

    def test_delete_returns_without_polling_when_already_gone(self, mocker) -> None:
        sleeper = mocker.patch("confluent_sql.artifacts.sleep_with_backoff")
        ctx = FakeContext(_response(status=204), _response(status=404))
        ArtifactsApi(ctx).delete(ARTIFACT_ID)
        assert [r[0] for r in ctx.requests] == ["DELETE", "GET"]
        sleeper.assert_not_called()

    def test_delete_wait_timeout_raises(self, mocker) -> None:
        mocker.patch("confluent_sql.artifacts.sleep_with_backoff", return_value=iter([None]))
        ctx = FakeContext(
            _response(status=204), _response(_artifact_body()), _response(_artifact_body())
        )
        with pytest.raises(OperationalError, match="was not removed within 5 seconds"):
            ArtifactsApi(ctx).delete(ARTIFACT_ID, timeout=5)

    def test_delete_wait_budget_includes_the_first_read(self, mocker) -> None:
        # The timeout is measured from before the first (possibly slow, retried) read.
        before_first_read, after_first_read = 100.0, 160.0
        mocker.patch(
            "confluent_sql.artifacts.time.monotonic",
            side_effect=[before_first_read, after_first_read],
        )
        sleeper = mocker.patch("confluent_sql.artifacts.sleep_with_backoff", return_value=iter([]))
        ctx = FakeContext(_response(status=204), _response(_artifact_body()))
        with pytest.raises(OperationalError):
            ArtifactsApi(ctx).delete(ARTIFACT_ID, timeout=5)
        sleeper.assert_called_once_with(5, started_at=before_first_read)

    def test_get_upload_url_returns_typed_target(self) -> None:
        ctx = FakeContext(_response(_presign_body()))
        target = ArtifactsApi(ctx)._get_upload_url("ZIP")
        assert target.upload_id == UPLOAD_ID
        assert ctx.requests[0][2]["json"] == {
            "content_format": "ZIP",
            "cloud": CLOUD,
            "region": REGION,
            "environment": ENV_ID,
        }

    def test_get_upload_url_rejects_unknown_format(self) -> None:
        with pytest.raises(InterfaceError, match="unknown content_format"):
            ArtifactsApi(FakeContext())._get_upload_url("TARBALL")
