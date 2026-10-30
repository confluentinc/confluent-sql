"""Dependency-light leaf helpers shared across the package.

This sits at (or very near) the child-most level of ``confluent_sql``, so it may import
only *very* carefully: strictly from modules that are themselves dependency-free leaves,
so that no import cycle can ever form no matter who imports ``utils``. In practice that
means the standard library plus ``confluent_sql.exceptions`` -- and ``exceptions`` is
itself leaf-clean (stdlib only at runtime), which is precisely what makes it safe to
raise the package's DB-API exceptions from here. Do not add an import to any module that
drags in further ``confluent_sql`` machinery; if a helper needs that, it belongs
elsewhere.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

import httpx

from confluent_sql.__version__ import __version__
from confluent_sql.exceptions import DataError

DEFAULT_HTTP_TIMEOUT_SECS: float = 10.0
"""Applied to the httpx client whenever a caller doesn't pass http_timeout_secs explicitly,
so the effective default is ours to control rather than an implicit dependency on httpx's own
default (5s). Lives here, a leaf module, so the OAuth provider can share it with `connection.py`
without an import cycle; `confluent_sql.connection` re-exports it."""

DEFAULT_USER_AGENT: str = (
    f"Confluent-SQL-Dbapi/v{__version__} (https://confluent.io; support@confluent.io)"
)
"""The User-Agent sent whenever a caller doesn't pass http_user_agent. Lives here for the same
reason as `DEFAULT_HTTP_TIMEOUT_SECS`; `Connection.DEFAULT_USER_AGENT` aliases it."""


class ExtensibleEnum(str, Enum):
    """Base for API enums the server may extend: an unrecognized value parses to `UNKNOWN`.

    A future server-side value then doesn't break response parsing. Subclasses must declare an
    `UNKNOWN` member; without one, an unrecognized value raises the usual `ValueError`.
    `(str, Enum)` rather than `StrEnum`, which needs Python 3.11+ (the package floor is 3.10).
    """

    @classmethod
    def _missing_(cls, value: object) -> ExtensibleEnum | None:
        """Return the `UNKNOWN` member for unrecognized values, or None if not present."""
        return cls.__members__.get("UNKNOWN")


def decode_sql_hex_literal(encoded: Any) -> bytes:
    """Decode a SQL ``x'..'`` hex-byte literal (e.g. ``x'7f0203'``) into ``bytes``.

    This is the wire form Flink uses for BINARY/VARBINARY payloads and for BYTES (and
    degraded-node) values inside a VARIANT payload, so both the VARBINARY and VARIANT
    decoders share it.

    Raises ``DataError`` if *encoded* is not a well-formed ``x'..'`` string: wrong type,
    missing wrapper, or invalid hex digits.
    """
    if not (isinstance(encoded, str) and encoded.startswith("x'") and encoded.endswith("'")):
        raise DataError(f"Expected an x'..'-encoded hex byte string but got {encoded!r}")
    try:
        return bytes.fromhex(encoded[2:-1])
    except ValueError as e:
        raise DataError(f"Invalid hex digits in x'..' byte string {encoded!r}: {e}") from e


def extract_error_detail(response: httpx.Response) -> str:
    """Extract server-provided error detail from an error response body.

    Understands three body shapes, in order of preference: the `errors[].detail` list (Flink,
    Tableflow, Artifacts), a top-level `message` string (Connect), and a non-JSON body, whose
    stripped text is used as-is. Falls back to "no more details" when none of these yields a
    non-empty string (e.g. an `errors` list that's empty, or whose entries omit `detail`).
    """
    try:
        body = response.json()
    except Exception:
        # Not JSON (e.g. a proxy's plain-text 502): the raw text is the only explanation there is.
        return response.text.strip() or "no more details"
    try:
        details = "; ".join(err["detail"] for err in body.get("errors", []) if err.get("detail"))
        message = body.get("message")
    except Exception:
        return "no more details"
    return details or (message if isinstance(message, str) and message else "no more details")


def next_page_token(next_url: str | None) -> str | None:
    """Extract the `page_token` from a response's `metadata.next` URL, if present.

    An empty/absent token collapses to None: pagination loops terminate on `is None`, so an empty
    string would spin them forever.
    """
    if next_url is None:
        return None
    return httpx.URL(next_url).params.get("page_token") or None
