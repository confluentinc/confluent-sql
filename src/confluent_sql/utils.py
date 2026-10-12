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

from typing import Any

import httpx

from confluent_sql.exceptions import DataError


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

    Falls back to "no more details" both when the body doesn't parse or carries
    no non-empty detail (an `errors` list that's empty, or whose entries omit `detail`).
    """
    try:
        errors = response.json().get("errors", [])
        details = "; ".join(err["detail"] for err in errors if err.get("detail"))
    except Exception:
        details = ""
    return details or "no more details"


def next_page_token(next_url: str | None) -> str | None:
    """Extract the `page_token` from a response's `metadata.next` URL, if present.

    An empty/absent token collapses to None: pagination loops terminate on `is None`, so an empty
    string would spin them forever.
    """
    if next_url is None:
        return None
    return httpx.URL(next_url).params.get("page_token") or None
