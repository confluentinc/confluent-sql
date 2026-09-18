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
