# Repository conventions for Claude

Guidance for any Claude working in this repository. Keep it short and concrete; add a convention
here only once the codebase actually follows it in more than one place.

## HTTP request routing: `ServiceClient`

Every HTTP request this driver makes goes through a `ServiceClient`
(`src/confluent_sql/service_client.py`), one per plane, held by `Connection`:

- `_dataplane` — the Flink gateway (statements and results).
- `_controlplane` — everything else: Tableflow, the CMK cluster lookup, the `/org/v2` organization
  lookup, Connect, Artifacts. One client and pool, built without credentials. Each request names
  the API it calls as a `ControlPlaneSurface` (`TABLEFLOW`, `CONNECT`, `ARTIFACTS`, `CMK`,
  `ORGANIZATION`) through `Connection._controlplane_request(surface, url, ...)`, which attaches
  that surface's credential (`_auth_for`, from `_surface_auth`) and applies its logging rule
  (Artifacts never logs response bodies: the presign response carries a live upload URL). CMK and
  organization lookups are global-key-only. A new surface is a new enum member, not a new client.

`ServiceClient.request(url, method="GET", raise_for_status=True, *, log_body=True, **kwargs)` is the
only way to send a request, and it decides retry from the method, so callers pick the plane (and,
on the control plane, the surface) they mean and never a retry policy:

- **GET is retried** via `call_with_retries()` (`src/confluent_sql/retry.py`) on transient transport
  errors (`httpx.NetworkError`, `httpx.RemoteProtocolError`; #137) and transient HTTP statuses
  (429/500/502/503/504, via `DEFAULT_RETRYABLE_STATUS_CODES`; #140), with a short exponential
  backoff before giving up. Timeouts are deliberately not retried; wait loops tolerate them via
  `poll_until` (#236), which also hands each attempt a Go-style `Context` (`context.py`) carrying
  the wait's deadline; a blocking result read does the same with its
  `statement_results_response_wallclock_secs` window. Pass it down explicitly as `ctx=` to the
  request, and `ServiceClient.request` caps that request's httpx timeouts at the time left (per
  phase: httpx has no total-request deadline), and a retried GET stops retrying once the deadline
  cannot outlast the next backoff (`call_with_retries(..., ctx=)`). A new wait's attempt should
  always forward its `ctx`. Timeouts are wall-clock budgets, never attempt counts. Status retries ride a private
  `_RetryableStatusError` sentinel, so `call_with_retries` itself stays HTTP-agnostic.
- **Every other method is sent exactly once**: re-issuing a POST/PATCH/DELETE after a dropped
  connection can double-submit or double-mutate state.

Never reach for the raw `httpx.Client` (`ServiceClient.httpx_client`) to send a request; that
bypasses retries, error translation (every `httpx.RequestError` becomes `OperationalError`, #138;
timeouts become `RequestTimeoutError`) and the reauth policy. Don't add a family of request
methods on `Connection`: the plane's `ServiceClient` (or `_controlplane_request`) is the entry point.

The retry design was introduced in #137 (see `CHANGELOG.md` 0.4.1) after a reported `ECONNRESET`
while polling a single statement; #140 added status retries and #235 extended them from the Flink
gateway to every control-plane GET. The reasoning, and the case for a plain function over a
decorator, is captured in `retry.py`'s module docstring.

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
- **Implement the wait with `poll_until`** from `polling.py`: it owns the backoff, the deadline and
  the `timeout` error, and treats one request timeout (`RequestTimeoutError`) as "no news yet"
  rather than a failure (#236). Don't hand-roll a sleep loop, and don't call `sleep_with_backoff`
  directly.

Exemplars: `Connection.stop_statement` / `Cursor.stop_statement` (`wait_for_stopped=True`),
`Connection.enable_tableflow` (`wait_for_running=True`), `Connection.disable_tableflow`
(`wait_for_removal=True`). The DDL convenience methods (`execute_snapshot_ddl`,
`execute_streaming_ddl`) block unconditionally and offer no opt-out — that's also acceptable when a
non-blocking variant would be meaningless.

## The `oauth` test marker

Every test module exercising the interactive OAuth login -- everything under `tests/unit/oauth/`,
`test_connection_oauth_unit.py`, and the `tests/integration/test_oauth*.py` modules -- carries
`pytest.mark.oauth` via module-level `pytestmark`, alongside its `unit`/`integration` marker. A new
OAuth test module must do the same. It is a topic axis orthogonal to unit/integration:

- `uv run pytest -m oauth` -- every OAuth test (integration ones skip without `RUN_OAUTH_TESTS`).
- `uv run pytest -m "oauth and unit"` -- the fast, browser-free subset.
- `RUN_OAUTH_TESTS=1 uv run pytest -n0 -m "oauth and integration"` -- the human-in-the-loop browser
  run. `-n0` is mandatory: the loopback callback port is fixed, so parallel workers collide.
