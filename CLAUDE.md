# Repository conventions for Claude

Guidance for any Claude working in this repository. Keep it short and concrete; add a convention
here only once the codebase actually follows it in more than one place.

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
