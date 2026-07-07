# Repository conventions for Claude

Guidance for any Claude working in this repository. Keep it short and concrete; add a convention
here only once the codebase actually follows it in more than one place.

## HTTP request routing: idempotent GETs vs mutating requests

`Connection` exposes two ways to issue an HTTP request (`src/confluent_sql/connection.py`):

- `_request_get(url, **kwargs)` — for idempotent GETs. Forwards `kwargs` (`params`, `headers`,
  `timeout`, etc.) to `_request`, forcing `method="GET"`, and wraps the whole call in
  `call_with_retries()` (`src/confluent_sql/retry.py`), which retries both transient transport
  errors (`httpx.NetworkError`, `httpx.RemoteProtocolError`; #137) and transient HTTP response
  statuses (429/500/502/503/504, via `DEFAULT_RETRYABLE_STATUS_CODES`; #140) with a short
  exponential backoff before giving up. Status-code retries are wired in via a private
  `_RetryableStatusError` sentinel raised inside `_request_get` -- `call_with_retries` itself
  stays HTTP-agnostic, unchanged from #137.
- `_request(url, method="GET", ...)` — for everything else, including POST/PATCH/DELETE.

**Every idempotent GET call site must go through `_request_get`, not `_request` directly.**
Re-issuing a GET after a dropped connection is safe; re-issuing a POST/PATCH/DELETE is not — it
can double-submit or double-mutate state. When adding a new GET call site (e.g. a future
`list_connectors()` or similar read-only endpoint), route it through `_request_get`. When adding a
mutating call site, use `_request` directly and do not wrap it in retry logic.

This distinction was introduced in #137 (see `CHANGELOG.md` 0.4.1) after a reported
`ECONNRESET` while polling a single statement via `get_statement()`/`_get_statement()`; the same
PR additionally retries `list_statements()`'s page-fetch loop and result-page fetching, since
those are equally idempotent GETs even though they weren't the reported failure. #140 extended
the same call sites to also retry on retryable HTTP status codes, not just transport exceptions.
The reasoning, and the case for a plain function over a decorator, is captured in `retry.py`'s
module docstring and `_request_get`'s docstring.

## Blocking vs. non-blocking lifecycle methods

Methods that drive a server-side resource toward a steady state (start a statement, stop it,
enable/disable a Tableflow sink, etc.) follow a uniform shape:

- **Block by default; return the settled state.** The common call should leave the resource in its
  intended terminal/ready state, not hand back a transient one the caller must then poll itself.
- **Expose a `wait_for_<settled-state>: bool = True` keyword-only argument.** The suffix names the
  exact condition awaited — *not* a generic `wait`/`block`. Existing names: `wait_for_stopped`
  (statement reaches STOPPED), `wait_for_running` (Tableflow topic reaches RUNNING),
  `wait_for_removal` (Tableflow topic is gone). A new method picks the suffix that is literally
  true of what it waits for.
- **Pair it with a uniform `timeout` keyword arg** (seconds; the standardized knob across all such
  methods).
- **`=False` is the opt-out**: return as soon as the request is accepted (resource typically in a
  PENDING/just-requested state).
- **While waiting, raise on terminal failure and on timeout** — surface the failure detail
  (`OperationalError`, e.g. a statement/topic that went FAILED), don't silently return a broken
  resource.
- **Implement the wait by polling with `sleep_with_backoff`** from `polling.py`; don't hand-roll a
  sleep loop.

Exemplars: `Connection.stop_statement` / `Cursor.stop_statement` (`wait_for_stopped=True`),
`Connection.enable_tableflow` (`wait_for_running=True`), `Connection.disable_tableflow`
(`wait_for_removal=True`). The DDL convenience methods (`execute_snapshot_ddl`,
`execute_streaming_ddl`) block unconditionally and offer no opt-out — that's also acceptable when a
non-blocking variant would be meaningless.
