"""Hardened HTTP + JSON primitives shared across the oauth package.

`token_chain.py`'s four exchanges all POST and parse a JSON response the same way; the callback
server (#152) and the refresh-loop provider (#153) landing later in this package will too. This
module centralizes that so every caller gets the same guarantee for free: a malformed-but-valid
server response (wrong JSON shape, missing field, wrong field type) becomes `OperationalError`,
never a bare `TypeError`/`AttributeError`/`KeyError` leaking past this package's DB-API boundary.

The recurring bug class this closes: code that calls `.get()` or subscripts a parsed JSON value
assuming it is a `dict`, when a syntactically-valid response can just as easily be a JSON list,
string, or number. `post_json` closes that at the one place every success path passes through, by
guaranteeing its return value is a `dict`; `require_field` and `optional_object_field` close it
for the field-level reads built on top.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx

from ..exceptions import OperationalError


def post_json(
    client: httpx.Client,
    url: str,
    *,
    check_response: Callable[[httpx.Response], None],
    **kwargs: Any,
) -> dict[str, Any]:
    """POST url and return its parsed JSON body as a dict, translating every failure mode into
    OperationalError: a transport-level failure (no response at all), check_response's own
    non-2xx handling, a 2xx body that isn't valid JSON, and a 2xx body that parses but isn't a
    JSON object (a list/string/number) are all normalized here -- so nothing downstream needs to
    guard against a non-dict body again."""
    try:
        response = client.post(url, **kwargs)
    except httpx.RequestError as e:
        raise OperationalError(f"error sending OAuth request: {type(e).__name__}: {e}") from e
    check_response(response)
    try:
        body = response.json()
    except ValueError as e:
        raise OperationalError(f"Could not parse OAuth response body as JSON: {e}") from e
    if not isinstance(body, dict):
        raise OperationalError(
            f"OAuth response body was valid JSON but not an object (got {type(body).__name__})"
        )
    return body


def require_field(
    body: dict[str, Any], field: str, *, context: str, type_: type | tuple[type, ...] = str
) -> Any:
    """Read body[field], raising OperationalError -- not KeyError/TypeError -- if the field is
    missing or not an instance of type_. A 2xx response missing or misshaping a field a caller
    depends on is a server-contract violation, which belongs in this driver's own Error
    hierarchy like every other OAuth chain failure, not a bare KeyError.

    bool is deliberately excluded unless explicitly requested via type_=bool (or a tuple
    containing it): Python's `isinstance(True, int)` is True, so a plain `type_=int` would
    otherwise silently accept a JSON `true`/`false` where a caller almost certainly means a
    genuine integer."""
    try:
        value = body[field]
    except KeyError as e:
        raise OperationalError(f"OAuth response for {context} is missing '{field}'") from e
    allowed = type_ if isinstance(type_, tuple) else (type_,)
    if not isinstance(value, allowed) or (isinstance(value, bool) and bool not in allowed):
        expected = " or ".join(t.__name__ for t in allowed)
        raise OperationalError(
            f"OAuth response for {context} has '{field}' of type {type(value).__name__}, "
            f"expected {expected}"
        )
    return value


def optional_object_field(body: dict[str, Any], field: str) -> dict[str, Any]:
    """Read body[field], defaulting to {} only if the key is absent entirely -- but raising
    OperationalError, not letting a caller's later .get() raise AttributeError, if the key is
    present with any non-object value, including JSON `null`. Use for optional nested blocks
    like /api/sessions' `organization`: an absent key means "no organization" (a valid, expected
    shape), but a present-and-null or present-and-wrong-typed key is still a server-contract
    violation worth surfacing, not something to quietly fold into the same default."""
    if field not in body:
        return {}
    value = body[field]
    if not isinstance(value, dict):
        raise OperationalError(
            f"OAuth response field '{field}' was present but not an object "
            f"(got {type(value).__name__})"
        )
    return value


def best_effort_json_object(response: httpx.Response) -> dict[str, Any]:
    """Parse response's body as JSON, returning it only if it decodes to an object; returns {}
    for a non-JSON body, an empty body, or JSON that decodes to a list/string/number. Never
    raises -- for best-effort error-message extraction on a response that's already about to
    produce an OperationalError, where a parse failure should just fall back to response.text
    rather than blocking on some other exception."""
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}
