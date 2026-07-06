# Project-specific instructions for confluent-sql

## HTTP request routing: idempotent GETs vs mutating requests

`Connection` exposes two ways to issue an HTTP request (`src/confluent_sql/connection.py`):

- `_request_get(url, params=None)` — for idempotent GETs. Wraps `call_with_retries()`
  (`src/confluent_sql/retry.py`), which retries transient transport errors (`httpx.NetworkError`,
  `httpx.RemoteProtocolError`) with a short exponential backoff before giving up.
- `_request(url, method="GET", ...)` — for everything else, including POST/PATCH/DELETE.

**Every idempotent GET call site must go through `_request_get`, not `_request` directly.**
Re-issuing a GET after a dropped connection is safe; re-issuing a POST/PATCH/DELETE is not — it
can double-submit or double-mutate state. When adding a new GET call site (e.g. a future
`list_connectors()` or similar read-only endpoint), route it through `_request_get`. When adding a
mutating call site, use `_request` directly and do not wrap it in retry logic.

This distinction was introduced in #137 (see `CHANGELOG.md` 0.4.1) after a reported
`ECONNRESET` on `list_statements()`; the reasoning and the case for a plain function over a
decorator is captured in `retry.py`'s module docstring and `_request_get`'s docstring.
