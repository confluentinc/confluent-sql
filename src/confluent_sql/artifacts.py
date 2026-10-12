"""
Flink artifact types, request builders and lifecycle orchestration (`/artifact/v1`).

Uploading is three steps -- request a presigned URL, POST the file to it (a plain object-store form
post, so no Confluent credentials), create the artifact with the resulting `upload_id` -- each
reachable from `Connection`; `ArtifactApi.create` runs all three. `ArtifactApi` depends on the
narrow `ArtifactContext` protocol rather than `Connection`, as `connectors.py` does.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePath
from typing import BinaryIO, Protocol, Union
from urllib.parse import quote

import httpx

from .exceptions import (
    ArtifactAlreadyExistsError,
    ArtifactNotFoundError,
    InterfaceError,
    OperationalError,
)
from .polling import sleep_with_backoff
from .retry import call_with_retries
from .types import StrAnyDict
from .utils import extract_error_detail, next_page_token

ArtifactFile = Union[str, "os.PathLike[str]", BinaryIO, bytes]
"""A path, an open binary file, or the raw bytes."""

_PRESIGNED_URL_LOCATION = "PRESIGNED_URL_LOCATION"
_MAX_DISPLAY_NAME_LENGTH = 60


@dataclass(frozen=True)
class UploadRetry:
    """Retry policy for the archive upload, split by what a retry costs: a failure to *connect*
    sent nothing, so retrying is cheap and on by default; a failure *mid-transfer* re-sends the
    whole archive, so it is off by default."""

    connect_retries: int = 3
    transfer_retries: int = 0

    def __post_init__(self) -> None:
        for name in ("connect_retries", "transfer_retries"):
            value = getattr(self, name)
            # bool is an int subclass; a float (inf, nan, 1.5) would also defeat attempt counting.
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise InterfaceError(f"{name} must be a non-negative integer, got {value!r}")


_CONNECT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)
_TRANSFER_ERRORS = (
    httpx.ReadError,
    httpx.WriteError,
    httpx.CloseError,
    httpx.RemoteProtocolError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
)


# Malformed-but-parseable payloads (null/wrong-typed fields) surface as these, not just KeyError.
_PARSE_ERRORS = (KeyError, AttributeError, TypeError, ValueError)


def _parse_error(what: str, e: Exception) -> OperationalError:
    detail = f"missing {e}" if isinstance(e, KeyError) else f"{type(e).__name__}: {e}"
    return OperationalError(f"Error parsing {what}, {detail}.")


def _json_body(response: httpx.Response, *, what: str) -> StrAnyDict:
    """The response's JSON object; a non-JSON or non-object 2xx body (e.g. a proxy's HTML page)
    is an OperationalError rather than a raw ValueError."""
    try:
        body = response.json()
    except ValueError as e:
        raise OperationalError(
            f"Error parsing {what}, response was not JSON "
            f"(HTTP {response.status_code}): {response.text[:200]!r}",
            http_status_code=response.status_code,
        ) from e
    if not isinstance(body, dict):
        raise OperationalError(
            f"Error parsing {what}, expected a JSON object, got {type(body).__name__}.",
            http_status_code=response.status_code,
        )
    return body


class Fields:
    """Wire field names (plain strings, not an Enum -- see `tableflow.Fields`)."""

    ID = "id"
    CLOUD = "cloud"
    REGION = "region"
    ENVIRONMENT = "environment"
    DISPLAY_NAME = "display_name"
    CLASS = "class"
    CONTENT_FORMAT = "content_format"
    DESCRIPTION = "description"
    DOCUMENTATION_LINK = "documentation_link"
    RUNTIME_LANGUAGE = "runtime_language"
    UPLOAD_SOURCE = "upload_source"
    LOCATION = "location"
    UPLOAD_ID = "upload_id"
    UPLOAD_URL = "upload_url"
    UPLOAD_FORM_DATA = "upload_form_data"
    VERSIONS = "versions"
    VERSION = "version"
    RELEASE_NOTES = "release_notes"
    IS_BETA = "is_beta"
    METADATA = "metadata"
    PAGE_SIZE = "page_size"
    PAGE_TOKEN = "page_token"


class _ExtensibleEnum(str, Enum):
    """Extensible API enum: an unrecognized value parses to `UNKNOWN` instead of raising."""

    @classmethod
    def _missing_(cls, value: object):
        return cls.__members__["UNKNOWN"]


class ArtifactContentFormat(_ExtensibleEnum):
    """Archive format of an artifact: a JAR for Java UDFs, a ZIP for Python ones."""

    JAR = "JAR"
    ZIP = "ZIP"
    UNKNOWN = "UNKNOWN"


class ArtifactRuntimeLanguage(_ExtensibleEnum):
    """Language runtime an artifact's code targets."""

    JAVA = "JAVA"
    PYTHON = "PYTHON"
    UNKNOWN = "UNKNOWN"


_EXTENSION_FORMATS = {".jar": ArtifactContentFormat.JAR, ".zip": ArtifactContentFormat.ZIP}


def infer_content_format(file: ArtifactFile) -> ArtifactContentFormat | None:
    """The format implied by a path's `.jar`/`.zip` extension, else None."""
    if isinstance(file, (str, os.PathLike)):
        return _EXTENSION_FORMATS.get(PurePath(os.fspath(file)).suffix.lower())
    return None


def _validate_display_name(display_name: str) -> None:
    if not isinstance(display_name, str) or not display_name.strip():
        raise InterfaceError("display_name must be a non-empty string")
    if len(display_name) > _MAX_DISPLAY_NAME_LENGTH:
        raise InterfaceError(
            f"display_name must be at most {_MAX_DISPLAY_NAME_LENGTH} characters, "
            f"got {len(display_name)}"
        )


def _normalize_content_format(value: ArtifactContentFormat | str) -> ArtifactContentFormat:
    try:
        fmt = ArtifactContentFormat(value)
    except ValueError as e:  # pragma: no cover - _missing_ makes this unreachable for str
        raise InterfaceError(f"unknown content_format: {value!r}") from e
    if fmt is ArtifactContentFormat.UNKNOWN:
        raise InterfaceError(f"unknown content_format: {value!r} (expected JAR or ZIP)")
    return fmt


def _normalize_runtime_language(value: ArtifactRuntimeLanguage | str) -> ArtifactRuntimeLanguage:
    lang = ArtifactRuntimeLanguage(value)
    if lang is ArtifactRuntimeLanguage.UNKNOWN:
        raise InterfaceError(f"unknown runtime_language: {value!r} (expected JAVA or PYTHON)")
    return lang


@dataclass(frozen=True)
class PresignedUploadUrl:
    """A presigned upload target for an artifact archive (`artifact.v1.PresignedUrl`).

    Valid for one hour. `upload_form_data` holds the form fields that must accompany the file. The
    echoed content_format/cloud/region/environment aren't reliably returned, so may be None.
    `upload_url` and `upload_form_data` are a live signed capability, so they're left out of `repr`.
    """

    upload_id: str
    upload_url: str = field(repr=False)
    upload_form_data: dict[str, str] = field(repr=False)
    content_format: ArtifactContentFormat | None
    cloud: str | None
    region: str | None
    environment: str | None
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> PresignedUploadUrl:
        try:
            return cls(
                upload_id=data[Fields.UPLOAD_ID],
                upload_url=data[Fields.UPLOAD_URL],
                upload_form_data={k: str(v) for k, v in data[Fields.UPLOAD_FORM_DATA].items()},
                content_format=ArtifactContentFormat(fmt)
                if (fmt := data.get(Fields.CONTENT_FORMAT))
                else None,
                cloud=data.get(Fields.CLOUD),
                region=data.get(Fields.REGION),
                environment=data.get(Fields.ENVIRONMENT),
                raw=data,
            )
        except _PARSE_ERRORS as e:
            raise _parse_error("presigned upload URL", e) from e


@dataclass(frozen=True)
class FlinkArtifactVersion:
    """One version of a Flink artifact (`artifact.v1.FlinkArtifactVersion`).

    The Cloud API currently supports only the single version the server assigns at creation.
    """

    version: str
    release_notes: str | None
    is_beta: bool | None
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> FlinkArtifactVersion:
        try:
            return cls(
                version=data[Fields.VERSION],
                release_notes=data.get(Fields.RELEASE_NOTES),
                is_beta=data.get(Fields.IS_BETA),
                raw=data,
            )
        except _PARSE_ERRORS as e:
            raise _parse_error("artifact version", e) from e


@dataclass(frozen=True)
class FlinkArtifact:
    """A Flink artifact as read back from the API (`artifact.v1.FlinkArtifact`).

    `raw` keeps the full response, including `metadata` (self link, CRN, timestamps). `versions`
    is populated only by `get_artifact`, not by create or list.
    """

    id: str
    display_name: str
    cloud: str
    region: str
    environment: str
    content_format: ArtifactContentFormat | None
    runtime_language: ArtifactRuntimeLanguage | None
    description: str | None
    documentation_link: str | None
    class_name: str | None
    versions: list[FlinkArtifactVersion]
    raw: StrAnyDict = field(repr=False)

    @classmethod
    def from_response(cls, data: StrAnyDict) -> FlinkArtifact:
        try:
            content_format = data.get(Fields.CONTENT_FORMAT)
            runtime_language = data.get(Fields.RUNTIME_LANGUAGE)
            return cls(
                id=data[Fields.ID],
                display_name=data[Fields.DISPLAY_NAME],
                cloud=data[Fields.CLOUD],
                region=data[Fields.REGION],
                environment=data[Fields.ENVIRONMENT],
                content_format=ArtifactContentFormat(content_format) if content_format else None,
                runtime_language=(
                    ArtifactRuntimeLanguage(runtime_language) if runtime_language else None
                ),
                description=data.get(Fields.DESCRIPTION),
                documentation_link=data.get(Fields.DOCUMENTATION_LINK),
                class_name=data.get(Fields.CLASS),
                versions=[
                    FlinkArtifactVersion.from_response(v) for v in data.get(Fields.VERSIONS) or []
                ],
                raw=data,
            )
        except _PARSE_ERRORS as e:
            raise _parse_error("artifact response", e) from e

    @property
    def latest_version(self) -> FlinkArtifactVersion | None:
        """The last version, or None if the response carried none."""
        return self.versions[-1] if self.versions else None


def build_presign_payload(
    *, content_format: ArtifactContentFormat, cloud: str, region: str, environment: str
) -> StrAnyDict:
    return {
        Fields.CONTENT_FORMAT: content_format.value,
        Fields.CLOUD: cloud,
        Fields.REGION: region,
        Fields.ENVIRONMENT: environment,
    }


def build_create_payload(
    *,
    display_name: str,
    upload_id: str,
    cloud: str,
    region: str,
    environment: str,
    content_format: ArtifactContentFormat | None = None,
    runtime_language: ArtifactRuntimeLanguage | None = None,
    description: str | None = None,
    documentation_link: str | None = None,
    class_name: str | None = None,
) -> StrAnyDict:
    """Unset optionals are omitted so the server's defaults (e.g. runtime_language JAVA) apply."""
    payload: StrAnyDict = {
        Fields.CLOUD: cloud,
        Fields.REGION: region,
        Fields.ENVIRONMENT: environment,
        Fields.DISPLAY_NAME: display_name,
        Fields.UPLOAD_SOURCE: {
            Fields.LOCATION: _PRESIGNED_URL_LOCATION,
            Fields.UPLOAD_ID: upload_id,
        },
    }
    optionals = {
        Fields.CONTENT_FORMAT: content_format.value if content_format else None,
        Fields.RUNTIME_LANGUAGE: runtime_language.value if runtime_language else None,
        Fields.DESCRIPTION: description,
        Fields.DOCUMENTATION_LINK: documentation_link,
        Fields.CLASS: class_name,
    }
    payload.update({k: v for k, v in optionals.items() if v is not None})
    return payload


def build_update_payload(
    *, description: str | None = None, documentation_link: str | None = None
) -> StrAnyDict:
    """None leaves a field unchanged; an empty string clears it. Raises InterfaceError if nothing
    is being changed."""
    payload = {
        k: v
        for k, v in {
            Fields.DESCRIPTION: description,
            Fields.DOCUMENTATION_LINK: documentation_link,
        }.items()
        if v is not None
    }
    if not payload:
        raise InterfaceError(
            "update_artifact requires at least one of description/documentation_link"
        )
    return payload


class ArtifactContext(Protocol):
    """What `ArtifactApi` needs from its host connection."""

    environment_id: str

    def artifact_scope(self) -> tuple[str, str]: ...

    def artifact_controlplane_request(
        self, url: str, method: str = "GET", raise_for_status: bool = True, **kwargs: object
    ) -> httpx.Response: ...


class ArtifactApi:
    """Artifact lifecycle behind `Connection`'s passthroughs. Create and update are synchronous;
    delete blocks by default (`wait_for_removal`)."""

    _ARTIFACTS_PATH = "/artifact/v1/flink-artifacts"
    _PRESIGN_PATH = "/artifact/v1/presigned-upload-url"

    def __init__(self, context: ArtifactContext) -> None:
        self._context = context

    def _artifact_path(self, artifact_id: str) -> str:
        """The item path, with `artifact_id` percent-encoded so it can't alter the route (e.g.
        `../x`); `.`/`..` survive quoting, so they're rejected."""
        if not isinstance(artifact_id, str) or artifact_id in ("", ".", ".."):
            raise InterfaceError(f"invalid artifact_id: {artifact_id!r}")
        return f"{self._ARTIFACTS_PATH}/{quote(artifact_id, safe='')}"

    def _scope_params(self, *, with_environment: bool = True) -> dict[str, str]:
        cloud, region = self._context.artifact_scope()
        params = {Fields.CLOUD: cloud, Fields.REGION: region}
        if with_environment:
            params[Fields.ENVIRONMENT] = self._context.environment_id
        return params

    def get_upload_url(self, content_format: ArtifactContentFormat | str) -> PresignedUploadUrl:
        """Request a presigned upload target."""
        cloud, region = self._context.artifact_scope()
        payload = build_presign_payload(
            content_format=_normalize_content_format(content_format),
            cloud=cloud,
            region=region,
            environment=self._context.environment_id,
        )
        response = self._context.artifact_controlplane_request(
            self._PRESIGN_PATH, method="POST", json=payload, raise_for_status=False
        )
        self._raise_for_status(response, prefix="Error requesting artifact upload URL")
        return PresignedUploadUrl.from_response(_json_body(response, what="presigned upload URL"))

    def upload(
        self,
        target: PresignedUploadUrl,
        file: ArtifactFile,
        *,
        upload_timeout: float = 300,
        retry: UploadRetry = UploadRetry(),  # noqa: B008 - frozen, safe to share
    ) -> None:
        """POST the archive to the object store with a credential-free client, so the Confluent API
        key is never sent there."""
        with _open_file(file) as (fileobj, filename):

            def post_once() -> httpx.Response:
                # httpx itself rewinds to 0 when building the body; `_open_file` guarantees 0 is
                # the start of the archive.
                fileobj.seek(0)
                with httpx.Client(timeout=upload_timeout) as client:
                    # Form fields before the file, as S3 requires.
                    return client.post(
                        target.upload_url,
                        data=target.upload_form_data,
                        files={"file": (filename, fileobj)},
                    )

            def post_retrying_connect() -> httpx.Response:
                return call_with_retries(
                    post_once, max_retries=retry.connect_retries, exceptions=_CONNECT_ERRORS
                )

            try:
                response = call_with_retries(
                    post_retrying_connect,
                    max_retries=retry.transfer_retries,
                    exceptions=_TRANSFER_ERRORS,
                )
            except httpx.RequestError as e:
                raise OperationalError(f"error uploading artifact: {type(e).__name__}: {e}") from e
        if not response.is_success:
            raise OperationalError(
                f"Error uploading artifact '{response.status_code}' - {response.text[:500]}",
                http_status_code=response.status_code,
            )

    def create(
        self,
        display_name: str,
        *,
        file: ArtifactFile | None = None,
        upload_id: str | None = None,
        content_format: ArtifactContentFormat | str | None = None,
        runtime_language: ArtifactRuntimeLanguage | str | None = None,
        description: str | None = None,
        documentation_link: str | None = None,
        class_name: str | None = None,
        upload_timeout: float = 300,
        upload_retry: UploadRetry = UploadRetry(),  # noqa: B008 - frozen, safe to share
    ) -> FlinkArtifact:
        """Create an artifact, uploading `file` first unless an `upload_id` is supplied."""
        if (file is None) == (upload_id is None):
            raise InterfaceError("create_artifact requires exactly one of file/upload_id")
        _validate_display_name(display_name)
        fmt = _normalize_content_format(content_format) if content_format is not None else None
        lang = (
            _normalize_runtime_language(runtime_language) if runtime_language is not None else None
        )

        if file is not None:
            fmt = fmt or infer_content_format(file)
            if fmt is None:
                raise InterfaceError(
                    "content_format is required when it can't be inferred from a .jar/.zip path"
                )
            target = self.get_upload_url(fmt)
            self.upload(target, file, upload_timeout=upload_timeout, retry=upload_retry)
            upload_id = target.upload_id
        assert upload_id is not None

        cloud, region = self._context.artifact_scope()
        payload = build_create_payload(
            display_name=display_name,
            upload_id=upload_id,
            cloud=cloud,
            region=region,
            environment=self._context.environment_id,
            content_format=fmt,
            runtime_language=lang,
            description=description,
            documentation_link=documentation_link,
            class_name=class_name,
        )
        response = self._context.artifact_controlplane_request(
            self._ARTIFACTS_PATH,
            method="POST",
            params=self._scope_params(with_environment=False),
            json=payload,
            raise_for_status=False,
        )
        # The API documents 409 for a taken name but in practice answers 400 "name should be unique
        # per Cloud/Region/Environment"; treat both as the duplicate they are.
        if response.status_code == 409 or (
            response.status_code == 400 and "should be unique" in response.text
        ):
            raise ArtifactAlreadyExistsError(
                f"Artifact '{display_name}' already exists",
                display_name=display_name,
                http_status_code=response.status_code,
            )
        self._raise_for_status(response, prefix="Error creating artifact")
        return FlinkArtifact.from_response(_json_body(response, what="artifact response"))

    def get(self, artifact_id: str) -> FlinkArtifact:
        """Read one artifact (the only call that returns its versions)."""
        response = self._context.artifact_controlplane_request(
            self._artifact_path(artifact_id),
            params=self._scope_params(),
            raise_for_status=False,
        )
        self._raise_if_not_found(response, artifact_id)
        self._raise_for_status(response, prefix="Error reading artifact")
        return FlinkArtifact.from_response(_json_body(response, what="artifact response"))

    def list_artifacts(
        self,
        *,
        runtime_language: ArtifactRuntimeLanguage | str | None = None,
        page_size: int = 100,
    ) -> list[FlinkArtifact]:
        """Every artifact in scope, following pagination."""
        params = self._scope_params()
        params[Fields.PAGE_SIZE] = str(page_size)
        if runtime_language is not None:
            params[Fields.RUNTIME_LANGUAGE] = _normalize_runtime_language(runtime_language).value

        artifacts: list[FlinkArtifact] = []
        while True:
            response = self._context.artifact_controlplane_request(
                self._ARTIFACTS_PATH, params=params, raise_for_status=False
            )
            self._raise_for_status(response, prefix="Error listing artifacts")
            body = _json_body(response, what="artifact list")
            artifacts.extend(FlinkArtifact.from_response(a) for a in body.get("data", []))
            token = next_page_token(body.get("metadata", {}).get("next"))
            if token is None:
                return artifacts
            params[Fields.PAGE_TOKEN] = token

    def update(
        self,
        artifact_id: str,
        *,
        description: str | None = None,
        documentation_link: str | None = None,
    ) -> FlinkArtifact:
        """Change mutable metadata in place."""
        payload = build_update_payload(
            description=description, documentation_link=documentation_link
        )
        response = self._context.artifact_controlplane_request(
            self._artifact_path(artifact_id),
            method="PATCH",
            params=self._scope_params(),
            json=payload,
            raise_for_status=False,
        )
        self._raise_if_not_found(response, artifact_id)
        self._raise_for_status(response, prefix="Error updating artifact")
        return FlinkArtifact.from_response(_json_body(response, what="artifact response"))

    def delete(
        self, artifact_id: str, *, wait_for_removal: bool = True, timeout: float = 300
    ) -> None:
        """Delete, then by default poll until a read 404s."""
        response = self._context.artifact_controlplane_request(
            self._artifact_path(artifact_id),
            method="DELETE",
            params=self._scope_params(),
            raise_for_status=False,
        )
        self._raise_if_not_found(response, artifact_id)
        self._raise_for_status(response, prefix="Error deleting artifact")
        if wait_for_removal:
            self._wait_for_removal(artifact_id, timeout)

    def _wait_for_removal(self, artifact_id: str, timeout: float) -> None:
        try:
            self.get(artifact_id)
        except ArtifactNotFoundError:
            return
        for _ in sleep_with_backoff(timeout):
            try:
                self.get(artifact_id)
            except ArtifactNotFoundError:
                return
        raise OperationalError(f"Artifact '{artifact_id}' was not removed within {timeout} seconds")

    @staticmethod
    def _raise_for_status(response: httpx.Response, *, prefix: str) -> None:
        if not response.is_success:
            raise OperationalError(
                f"{prefix} '{response.status_code}' - {extract_error_detail(response)}",
                http_status_code=response.status_code,
            )

    @staticmethod
    def _raise_if_not_found(response: httpx.Response, artifact_id: str) -> None:
        if response.status_code == 404:
            raise ArtifactNotFoundError(
                f"Artifact '{artifact_id}' does not exist",
                artifact_id=artifact_id,
                http_status_code=response.status_code,
            )


class _open_file:
    """Yields `(binary file, filename)` for any `ArtifactFile`, always seekable with the archive
    starting at offset 0; closes only files it opened.

    A caller-supplied file object is read from its *current position* to the end into memory.
    httpx rewinds file objects to 0 and sizes the body from the whole file, so a mid-stream
    position would otherwise upload the wrong bytes, and a non-seekable stream (pipe, gzip reader)
    would have no known length, which presigned object-store POSTs reject."""

    def __init__(self, file: ArtifactFile) -> None:
        self._file = file
        self._opened: BinaryIO | None = None

    def __enter__(self) -> tuple[BinaryIO, str]:
        file = self._file
        if isinstance(file, (str, os.PathLike)):
            try:
                self._opened = open(file, "rb")  # noqa: SIM115 - closed in __exit__
            except OSError as e:
                raise InterfaceError(f"can't read artifact file {file!r}: {e}") from e
            return self._opened, PurePath(os.fspath(file)).name
        if isinstance(file, (bytes, bytearray)):
            return io.BytesIO(file), "artifact"
        name = getattr(file, "name", None)
        try:
            data = file.read()
        except (OSError, ValueError) as e:
            raise InterfaceError(f"can't read artifact file object: {e}") from e
        if not isinstance(data, (bytes, bytearray)):
            raise InterfaceError("artifact file object must be opened in binary mode")
        return io.BytesIO(data), PurePath(name).name if isinstance(name, str) else "artifact"

    def __exit__(self, *exc_info: object) -> None:
        if self._opened is not None:
            self._opened.close()
