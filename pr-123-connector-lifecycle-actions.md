# Connector lifecycle actions: pause / resume

The next child of #121 on top of the core lifecycle from #122 / PR #131: two management actions
with no Tableflow analog, each blocking to its settled state per the repo's `wait_for_<settled-state>`
convention.

## The two actions

- `pause_connector(name, *, wait_for_paused=True, timeout=...)` — `PUT .../connectors/{name}/pause`,
  settles at **PAUSED**.
- `resume_connector(name, *, wait_for_running=True, timeout=...)` — `PUT .../connectors/{name}/resume`,
  settles at **RUNNING**.
- Each is a one-line `Connection` passthrough to a new `ConnectorApi.pause`/`resume`, and returns the
  settled `Connector`. The lifecycle action responses carry no connector body, so the settled state
  is read back via the existing `get` (the config-read + `/status`-read merge) — a
  `_settle_after_action` helper does that read and then either returns the just-requested state
  (`wait=False`) or blocks. A shared `_connector_action` helper issues the `PUT` and maps
  404 → `ConnectorNotFoundError`, other HTTP → `OperationalError`, mirroring `delete`/`_read_spec`.
- Verbs/paths/codes verified against the Connect v1 OpenAPI spec (`api/connect/openapi.yaml`):
  both are `PUT`, both return `202 Accepted` (asynchronous — the wait loop polling `/status` is
  exactly what the async contract calls for), 404 → `ResourceNotFoundError`, no request body or
  query params.

## Generalized connector wait

- `_wait_for_running` becomes `_wait_for_state(connector, target, timeout)`: the poll loop now stops
  at an arbitrary `target` state (RUNNING for create/resume, PAUSED for pause), still raising on
  FAILED with `status.trace` and on timeout (the timeout message now names the awaited
  `target.value`). `create` passes `ConnectorState.RUNNING`; the three waits share the one loop.
- This **supersedes the `ConnectorState.is_terminal` property** that PR #133's follow-up resurrected.
  "Terminal" (RUNNING/FAILED) is a create-wait notion, not a state-intrinsic one — it can't express
  pause's PAUSED target — so a target-parameterized predicate replaces it, and `is_terminal` (plus
  its unit test) is removed as the dead code it again becomes. This branch is based on PR #133 and
  merges after it.

## Tests

- `ConnectorApi` actions are unit-tested against the existing fake `ControlPlaneContext` (its router
  extended to dispatch the `/pause` `/resume` sub-paths), parametrized across both actions: correct
  verb+path, blocks-until-target, `wait=False` returns after one `get`, 404 → not-found, 500 →
  operational, FAILED-during-wait raises with trace, and timeout naming the target.
- Connection passthrough delegation is unit-tested (parametrized over both methods).
- The env-gated Datagen integration arc now threads pause → PAUSED, resume → RUNNING between the
  create/get and the delete, reusing the one expensive create/delete.

## Relationships

- Closes #123.
- Child of #121.
- Builds on #122 / PR #131 (the `ConnectorApi` + `ControlPlaneContext` seam) and is sequenced after
  PR #133 (whose `settled()`/`is_terminal` it generalizes).
