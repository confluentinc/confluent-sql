"""Decoding of Flink VARIANT result payloads into typed Python values.

A VARIANT value is self-describing: unlike every other type, its element types are not
in the schema but inline in the payload, as a tree of positional ``[code, ...]`` nodes.
:func:`_decode_variant_node` walks that tree and reconstructs the typed Python value -- an
OBJECT becomes a dict, an ARRAY a list, and each scalar its matching Python type, parsed
the same way the same-typed top-level column would be. It is driven from
:class:`confluent_sql.types.VariantConverter`.

Nanosecond-precision timestamps (``TIMESTAMP_NS`` / ``TIMESTAMP_LTZ_NS``) are truncated
(not rounded) to microseconds, since Python's ``datetime`` cannot represent finer
resolution. Every other value is decoded losslessly.

The walk is recursive and deliberately unbounded, with no depth guard. That is safe
because by the time a node reaches this module the payload has already been fully
parsed from the HTTP response by ``response.json()`` (stdlib ``json.loads``), which is
itself recursive over the same nesting -- each node is ``[code, [...]]``, so its JSON
form nests at least as deep as this walk does. A payload deep enough to overflow this
recursion would therefore have overflowed ``json.loads`` first and never have produced
the Python ``list`` we are handed. In other words, receiving a finite parsed structure
here is proof that its depth is already within the interpreter's recursion limit. (The
paths that build the payload -- a Kafka producer's schema serializer, or Flink's own
JSON parser for a ``PARSE_JSON`` literal -- bound the depth well below that besides.)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, TypeAlias

from confluent_sql.exceptions import DataError
from confluent_sql.utils import decode_sql_hex_literal


@dataclass(frozen=True)
class UndecodableVariant:
    """A VARIANT node the driver could not decode into a typed Python value.

    The backend degrades such a node rather than failing the whole row, attaching the
    raw variant bytes so a newer decoder can still recover the value:

    - code ``-1`` (UNKNOWN): a node whose variant type id this driver does not
      recognize -- the forward-compatibility path for types added server-side later.
    - code ``-2`` (INVALID): a structurally broken subtree.

    ``metadata`` is the variant's shared metadata dictionary and ``value`` the node's
    own value bytes; either may be empty when nothing could be recovered.
    """

    code: int
    metadata: bytes
    value: bytes


# VARIANT wire type codes: a fixed wire contract, deliberately NOT derived from Flink's
# Variant.Type ordinal so an upstream reorder can't change them. Each payload node is a
# JSON array whose first element is one of these codes; the sign of the code gives the
# node's shape (< 0 degraded [code, metaHex, valueHex], 0 NULL [0], > 0 a value
# [code, value] -- the LTZ timestamps are [code, utcTimestamp, sessionOffset]).
_VARIANT_INVALID = -2
_VARIANT_UNKNOWN = -1
_VARIANT_NULL = 0
_VARIANT_OBJECT = 1
_VARIANT_ARRAY = 2
_VARIANT_BOOLEAN = 3
_VARIANT_TINYINT = 4
_VARIANT_SMALLINT = 5
_VARIANT_INT = 6
_VARIANT_BIGINT = 7
_VARIANT_FLOAT = 8
_VARIANT_DOUBLE = 9
_VARIANT_DECIMAL = 10
_VARIANT_STRING = 11
_VARIANT_DATE = 12
_VARIANT_TIMESTAMP = 13
_VARIANT_TIMESTAMP_LTZ = 14
_VARIANT_BYTES = 15
_VARIANT_TIME = 16
_VARIANT_TIMESTAMP_NS = 17
_VARIANT_TIMESTAMP_LTZ_NS = 18


def _cap_variant_microseconds(value: str) -> str:
    """Truncate a VARIANT timestamp's fractional seconds to microseconds.

    Only the nanosecond timestamp variants (TIMESTAMP_NS / TIMESTAMP_LTZ_NS) carry 9
    fractional digits; Python's datetime holds only microseconds, so the extra precision
    is dropped. The other temporal types fit in microseconds and are decoded directly.
    """
    head, dot, fractional = value.partition(".")
    if not dot:
        return value
    return f"{head}.{fractional[:6]}"


def _decode_variant_utc_timestamp(value: str) -> datetime:
    """Decode a VARIANT LTZ timestamp value (at UTC) as a UTC-aware datetime.

    The wire node also carries the session-zone offset, but the driver drops it and
    returns UTC, mirroring how a TIMESTAMP_LTZ column is returned.
    """
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _decode_variant_boolean(value: str) -> bool:
    """Decode a VARIANT BOOLEAN value (``"TRUE"`` / ``"FALSE"``) to a Python bool."""
    if value == "TRUE":
        return True
    if value == "FALSE":
        return False
    raise DataError(f"Invalid VARIANT BOOLEAN value: {value!r}")


# Decoders for the scalar VARIANT type codes, keyed by code. Each takes the node's value
# string (node[1]) and returns the typed Python value. Structural codes (NULL, OBJECT,
# ARRAY) and degraded codes (< 0) are handled directly by the walker, not here.
_VARIANT_SCALAR_DECODERS: dict[int, Callable[[str], VariantValue]] = {
    _VARIANT_BOOLEAN: _decode_variant_boolean,
    _VARIANT_TINYINT: int,
    _VARIANT_SMALLINT: int,
    _VARIANT_INT: int,
    _VARIANT_BIGINT: int,
    _VARIANT_FLOAT: float,
    _VARIANT_DOUBLE: float,
    _VARIANT_DECIMAL: Decimal,
    _VARIANT_STRING: lambda value: value,
    _VARIANT_DATE: date.fromisoformat,
    _VARIANT_TIME: time.fromisoformat,
    _VARIANT_TIMESTAMP: datetime.fromisoformat,
    _VARIANT_TIMESTAMP_LTZ: _decode_variant_utc_timestamp,
    # The nanosecond variants carry 9 fractional digits; cap them to microseconds first.
    _VARIANT_TIMESTAMP_NS: lambda value: datetime.fromisoformat(_cap_variant_microseconds(value)),
    _VARIANT_TIMESTAMP_LTZ_NS: lambda value: _decode_variant_utc_timestamp(
        _cap_variant_microseconds(value)
    ),
    _VARIANT_BYTES: decode_sql_hex_literal,
}


VariantValue: TypeAlias = (
    bool
    | int
    | float
    | Decimal
    | str
    | date
    | time
    | datetime
    | bytes
    | list
    | dict
    | UndecodableVariant
    | None
)
"""The Python value a VARIANT node decodes to: a scalar with its Flink type preserved
(bool, int, float, Decimal, str, date, time, datetime, bytes), a list (ARRAY), a dict
(OBJECT), None (NULL), or UndecodableVariant for a node that could not be read."""


def _decode_variant_node(node: Any) -> VariantValue:
    """Recursively decode one ``[code, ...]`` VARIANT payload node into its Python value.

    Nanosecond-precision timestamps are truncated to microseconds (see the module
    docstring); every other value is decoded losslessly.
    """
    if not isinstance(node, list) or not node:
        raise DataError(f"Malformed VARIANT node, expected a non-empty list: {node!r}")

    code = node[0]
    if not isinstance(code, int) or isinstance(code, bool):
        raise DataError(f"Malformed VARIANT node, type code is not an integer: {node!r}")

    if code == _VARIANT_NULL:
        return None

    if code < 0:
        # Degraded node: [code, metadataHex, valueHex], each possibly "x''" (empty).
        metadata = decode_sql_hex_literal(node[1]) if len(node) > 1 else b""
        value = decode_sql_hex_literal(node[2]) if len(node) > 2 else b""
        return UndecodableVariant(code=code, metadata=metadata, value=value)

    if code == _VARIANT_OBJECT:
        return _decode_variant_object(node)

    if code == _VARIANT_ARRAY:
        return _decode_variant_array(node)

    decoder = _VARIANT_SCALAR_DECODERS.get(code)
    if decoder is None:
        raise DataError(f"Unhandled VARIANT type code {code} in node {node!r}")

    if len(node) < 2:
        raise DataError(f"VARIANT scalar node of type code {code} has no value: {node!r}")
    value = node[1]
    try:
        return decoder(value)
    except DataError:
        raise
    except Exception as e:
        raise DataError(f"Invalid VARIANT scalar value for type code {code}: {value!r}") from e


def _decode_variant_object(node: list) -> dict[str, VariantValue]:
    """Decode an OBJECT node ``[1, [[key, node], ...]]`` into a dict.

    An empty object is ``[1, []]``; the field-list element is always present, so a bare
    ``[1]`` is malformed rather than an empty object.
    """
    if len(node) < 2:
        raise DataError(f"Malformed VARIANT object, missing field list: {node!r}")
    fields = node[1]
    if not isinstance(fields, list):
        raise DataError(f"Malformed VARIANT object, expected a list of fields: {node!r}")
    result: dict[str, VariantValue] = {}
    for pair in fields:
        if not (isinstance(pair, list) and len(pair) == 2):
            raise DataError(f"Malformed VARIANT object field, expected [key, node]: {pair!r}")
        key, child = pair
        if not isinstance(key, str):
            raise DataError(f"Malformed VARIANT object field, expected a string key: {pair!r}")
        result[key] = _decode_variant_node(child)
    return result


def _decode_variant_array(node: list) -> list[VariantValue]:
    """Decode an ARRAY node ``[2, [node, ...]]`` into a list.

    An empty array is ``[2, []]``; the element-list element is always present, so a bare
    ``[2]`` is malformed rather than an empty array.
    """
    if len(node) < 2:
        raise DataError(f"Malformed VARIANT array, missing element list: {node!r}")
    elements = node[1]
    if not isinstance(elements, list):
        raise DataError(f"Malformed VARIANT array, expected a list of nodes: {node!r}")
    return [_decode_variant_node(child) for child in elements]
