"""Example: interactive OAuth (epic #150) plus a long-running append-only streaming query, to
gain confidence that the Flink data-plane token is silently refreshed once it outlives its
initial (~5 minute in production) lifetime, rather than the connection dying at that point --
and to measure just how much (or how little) overhead those in-loop refreshes add for a
realistic long-lived client.

Four independent ways to observe the refresh while this runs:

1. INFO-level logging is enabled below, so httpx logs every request it makes at INFO
   ("HTTP Request: METHOD url ..."). The polling loop below only ever talks to the Flink
   gateway (`.../statements/.../results`); an *unprompted* extra request to the auth service's
   `/oauth/token` endpoint appearing on its own between polls is the provider's on-request
   refresh firing transparently.
2. This script also peeks directly at the shared provider's current data-plane token expiry
   (`conn._oauth_provider.token_set.dp_token_expires_at` -- internal, not a supported/public API,
   used here purely for this demo's own observability) after every poll, and prints a distinct
   line the moment that expiry changes -- unambiguous proof a refresh happened, independent of
   log parsing.
3. `conn.oauth_metrics` (supported, public API) reports how many in-loop refresh chains have run
   and how long each of their three network hops took, updated after every poll and summarized as
   a percentage of this script's total wall-clock runtime when it exits.
4. This script also installs its own httpx event hooks on the Flink client
   (`conn._get_flink_client()` -- internal, not a supported/public API, purely for this demo's
   own observability, same as point 2) to count and time every actual Flink gateway request the
   driver makes. Compared against point 3's token-exchange hit count/time, this is what answers
   "what fraction of our traffic to Confluent Cloud, by hit count and by time, was token-refresh
   machinery rather than real Flink work" -- e.g. "20 token-exchange hits / 5s, out of 575 total
   hits / 340s".

Leave this running for several minutes (past the data-plane token's initial lifetime) to see
either signal fire; nothing else needs to happen for the refresh to occur; it's driven purely by
this script's own periodic requests. If you leave it running long enough to cross the ~8h
absolute session wall, the request that crosses it will raise ReauthenticationRequired once and
recover on its own on the very next request -- auth="oauth" defaults to
oauth_policy=confluent_sql.oauth.OAUTH_INTERACTIVE (on_reauthentication_required="auto") -- which
pops a second browser login; watch for that too if you go the distance (it does not count towards
`oauth_metrics`, which covers only in-loop refreshes, not interactive logins).

Every 4th poll, this script also inserts a few rows of its own (via a second, short-lived cursor
on the same connection) so the polling loop has something new to report without requiring any
manual intervention -- see them appear a few seconds after each such insert below.
"""

import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

import confluent_sql
from confluent_sql.oauth import CCloudOAuth, OAuthMetrics
from confluent_sql.statement import Statement

POLL_INTERVAL_SECS = 15
INSERT_EVERY_N_POLLS = 4

RANDOM_NAMES = ["Dave", "Erin", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Mallory", "Peggy"]
"""Extending the classic Alice/Bob/Charlie cast for the rows this script inserts itself."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _insert_random_rows(conn: confluent_sql.Connection, table_name: str) -> int:
    """Insert a few rows with random ids/names into `table_name`, via a fresh cursor -- safe to
    call while a separate streaming cursor on the same connection is mid-query, since each is an
    independent Flink SQL statement over the same HTTP connection."""
    count = random.randint(2, 4)
    values = ", ".join(
        f"({random.randint(1000, 9999)}, '{random.choice(RANDOM_NAMES)}')" for _ in range(count)
    )
    with conn.closing_cursor() as insert_cursor:
        insert_cursor.execute(f"INSERT INTO {table_name} (id, name) VALUES {values}")
    return count


def _drain_and_print(cursor: confluent_sql.Cursor) -> None:
    """Print every row currently available from `cursor`, looping `fetchmany()` until it comes
    back empty. A single `fetchmany()` call only returns up to `cursor.arraysize` rows (1 by
    default) even when the server handed back a bigger page in that one request, so this loops
    to drain everything already fetched before returning -- otherwise a burst of several rows
    would trickle out one per poll instead of all appearing together."""
    while True:
        rows = cursor.fetchmany()
        if not rows:
            return
        for row in rows:
            print(f"{_now()} {row}")


def _dp_token_expiry(conn: confluent_sql.Connection) -> datetime | None:
    """Peek at the shared OAuth provider's current data-plane token expiry, purely so this
    example can detect (and announce) a silent refresh. Not a supported/public API."""
    provider = conn._oauth_provider
    if not isinstance(provider, CCloudOAuth):
        return None
    snapshot = provider.token_set
    return snapshot.dp_token_expires_at if snapshot is not None else None


class _FlinkHitMetrics:
    """Counts and times every request the driver's Flink client actually sends, via httpx event
    hooks installed directly on it (see this file's docstring, point 4) -- this script's own
    instrumentation of "everything that isn't a token exchange," so it can be compared against
    `conn.oauth_metrics`. Not driver API; a real caller has no equivalent hook point today."""

    def __init__(self) -> None:
        self.count = 0
        self.total_secs = 0.0

    def install(self, client: httpx.Client) -> None:
        client.event_hooks["request"].append(self._on_request)
        client.event_hooks["response"].append(self._on_response)

    @staticmethod
    def _on_request(request: httpx.Request) -> None:
        request.extensions["_hit_metrics_start"] = time.monotonic()

    def _on_response(self, response: httpx.Response) -> None:
        # httpx calls "response" hooks as soon as headers arrive, before the body is read --
        # stopping the clock here would time only the round-trip to headers, while the OAuth
        # exchange functions this is compared against (`post_json` -> `response.json()`) also
        # include body-read/parse time. Force the read now so both sides measure the same thing:
        # request sent to response fully consumed. Harmless if the driver reads it again later --
        # httpx caches the body on first read.
        response.read()
        self.count += 1
        start = response.request.extensions.get("_hit_metrics_start")
        if start is not None:
            self.total_secs += time.monotonic() - start


def _oauth_hit_count_and_secs(metrics: OAuthMetrics) -> tuple[int, float]:
    """Reduce `OAuthMetrics`'s three per-hop fields down to one (hits, seconds) pair -- each
    successful hop is one HTTP request, so summing all three gives the total token-exchange
    traffic, directly comparable to `_FlinkHitMetrics`'s per-request counting."""
    hits = metrics.refresh_leg_count + metrics.cp_exchange_count + metrics.dp_exchange_count
    secs = metrics.refresh_leg_secs + metrics.cp_exchange_secs + metrics.dp_exchange_secs
    return hits, secs


def _print_metrics(conn: confluent_sql.Connection, flink_hits: _FlinkHitMetrics) -> None:
    """Print running totals comparing token-exchange traffic against actual Flink gateway
    traffic, by both hit count and time spent -- the answer to "what fraction of our traffic to
    Confluent Cloud is token-refresh machinery, versus real work"."""
    metrics = conn.oauth_metrics
    if metrics is None:
        return
    oauth_hits, oauth_secs = _oauth_hit_count_and_secs(metrics)
    total_hits = oauth_hits + flink_hits.count
    total_secs = oauth_secs + flink_hits.total_secs
    hit_pct = (oauth_hits / total_hits * 100) if total_hits else 0.0
    secs_pct = (oauth_secs / total_secs * 100) if total_secs else 0.0
    print(
        f"{_now()} oauth_metrics: {metrics.refresh_chain_count} refresh(es) succeeded, "
        f"{metrics.failed_refresh_chain_count} failed ({metrics.failed_refresh_chain_secs:.3f}s) "
        f"-- {oauth_hits} token-exchange hit(s)/{oauth_secs:.3f}s vs. "
        f"{flink_hits.count} Flink hit(s)/{flink_hits.total_secs:.3f}s "
        f"({hit_pct:.1f}% of hits, {secs_pct:.1f}% of time on successful token exchanges)"
    )


def _print_overhead_summary(
    conn: confluent_sql.Connection, flink_hits: _FlinkHitMetrics, run_secs: float
) -> None:
    """Final answer to "how much overhead do OAuth refreshes add": both as a fraction of this
    script's total wall-clock runtime, and as a fraction of all the hits/time it took to talk to
    Confluent Cloud at all."""
    metrics = conn.oauth_metrics
    if metrics is None:
        return
    oauth_hits, oauth_secs = _oauth_hit_count_and_secs(metrics)
    total_hits = oauth_hits + flink_hits.count
    total_secs = oauth_secs + flink_hits.total_secs
    # Includes failed-chain time -- a chain that blocked until an HTTP timeout before failing
    # cost real wall-clock time and must count toward "how much did OAuth cost us", even though
    # (unlike a successful chain's hops) it isn't attributed to a specific hop below.
    oauth_wall_secs = metrics.refresh_chain_secs + metrics.failed_refresh_chain_secs
    wall_pct = (oauth_wall_secs / run_secs * 100) if run_secs > 0 else 0.0
    hit_pct = (oauth_hits / total_hits * 100) if total_hits else 0.0
    secs_pct = (oauth_secs / total_secs * 100) if total_secs else 0.0
    print(
        f"{_now()} Ran for {run_secs:.1f}s; spent {oauth_wall_secs:.3f}s ({wall_pct:.3f}% of "
        f"wall clock) on OAuth refresh chains -- {metrics.refresh_chain_count} succeeded "
        f"({metrics.refresh_chain_secs:.3f}s), {metrics.failed_refresh_chain_count} failed "
        f"({metrics.failed_refresh_chain_secs:.3f}s)."
    )
    print(
        f"{_now()} Of {total_hits} total hits to Confluent Cloud ({total_secs:.3f}s), "
        f"{oauth_hits} ({hit_pct:.1f}%) / {oauth_secs:.3f}s ({secs_pct:.1f}%) were successful "
        f"token exchanges; the remaining {flink_hits.count} / {flink_hits.total_secs:.3f}s were "
        "actual Flink gateway requests. (Failed refresh attempts add to the wall-clock total "
        "above but aren't broken out by hop here.)"
    )


# Surfaces httpx's own per-request INFO logs (see this file's docstring, point 1) plus the
# driver's own sign-in/re-authentication INFO logs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

conn = confluent_sql.connect(
    auth="oauth",
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
    database=os.getenv("CONFLUENT_TEST_DBNAME", "default"),
)
print(f"{_now()} Signed in to Confluent Cloud for organization {conn.organization_id!r}")
run_start = time.monotonic()
"""Anchors the final overhead summary -- deliberately after login, since that's a one-time cost
this example isn't measuring; see `oauth_metrics`/`_print_overhead_summary`."""

flink_hits = _FlinkHitMetrics()
flink_hits.install(conn._get_flink_client())  # noqa: SLF001 -- see this file's docstring, point 4
"""Installed before the very first Flink request (the CREATE TABLE below), so every hit this
connection makes to the Flink gateway for the rest of the run is counted."""

table_name = f"oauth_refresh_demo_{uuid.uuid4().hex[:8]}"
table_created = False
"""Guards the DROP TABLE in the final `finally` -- CREATE TABLE below is inside the same guarded
region as everything else now, so a failure there must not attempt to drop a table that was
never (or only partially) created."""

streaming_statement: Statement | None = None
"""Retained so cleanup can stop the streaming statement even after the Ctrl+C path has already
closed the cursor that ran it -- see the `finally` block below. Kept as a `Statement` object
(refreshed after every poll), not just its name, so `stop_statement()` can use its client-side
terminal-phase short-circuit instead of blindly re-issuing a stop against a statement that may
have already failed on its own."""

try:
    with conn.closing_cursor() as cursor:
        print(f"{_now()} Creating table {table_name} ...")
        cursor.execute(
            f"""
            CREATE TABLE {table_name} (
                id INT,
                name STRING
            )
            """
        )
        table_created = True
        print(f"{_now()} Inserting initial sample rows into {table_name} ...")
        cursor.execute(
            f"""
            INSERT INTO {table_name} (id, name) VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
            """
        )

    last_dp_expiry = _dp_token_expiry(conn)
    print(f"{_now()} Initial data-plane token expires at {last_dp_expiry}")

    print(f"{_now()} Starting streaming query against {table_name} ...")
    with conn.closing_streaming_cursor() as cursor:
        try:
            cursor.execute(f"SELECT id, name FROM {table_name}")
        finally:
            # execute() submits the statement server-side, then blocks waiting for it to become
            # ready -- capture whatever got submitted even if that wait times out or raises, so
            # cleanup below can still find and stop it. cursor._statement is set as soon as
            # submission succeeds, before the wait begins.
            streaming_statement = cursor._statement  # noqa: SLF001
        assert cursor.statement.is_append_only, "expected an append-only streaming statement"

        print(f"{_now()} Draining initial results ...")
        _drain_and_print(cursor)
        streaming_statement = cursor.statement

        print(
            f"{_now()} No more results immediately available -- polling every "
            f"{POLL_INTERVAL_SECS}s from here on, inserting a few random rows every "
            f"{INSERT_EVERY_N_POLLS} polls. Leave this running for several minutes to observe a "
            "silent data-plane token refresh. Ctrl+C to stop."
        )

        poll_count = 0
        while cursor.may_have_results:
            print(f"{_now()} sleeping for {POLL_INTERVAL_SECS}s ...")
            time.sleep(POLL_INTERVAL_SECS)
            poll_count += 1

            if poll_count % INSERT_EVERY_N_POLLS == 0:
                inserted = _insert_random_rows(conn, table_name)
                print(f"{_now()} Inserted {inserted} random row(s) into {table_name}")

            _drain_and_print(cursor)
            streaming_statement = cursor.statement

            current_dp_expiry = _dp_token_expiry(conn)
            if current_dp_expiry != last_dp_expiry:
                print(
                    f"{_now()} *** data-plane token was refreshed -- new expiry: "
                    f"{current_dp_expiry} ***"
                )
                last_dp_expiry = current_dp_expiry

            _print_metrics(conn, flink_hits)

        print(
            f"{_now()} Statement is no longer producing results "
            f"(phase={cursor.statement.phase}); exiting."
        )
except KeyboardInterrupt:
    print(f"\n{_now()} Interrupted; cleaning up ...")
finally:
    # conn.close() must run no matter what -- nested in its own finally so a failure in either
    # cleanup step above it (the metrics summary, or the DROP TABLE) can't skip releasing the
    # connection/provider.
    try:
        _print_overhead_summary(conn, flink_hits, time.monotonic() - run_start)
        if streaming_statement is not None:
            # On the (expected) Ctrl+C path, this statement is still RUNNING server-side --
            # `closing_streaming_cursor()`'s exit only calls `Cursor.close()`, which explicitly
            # does not stop an active statement (see Cursor.close()'s docstring). Left running,
            # it would still be reading from `table_name` when the DROP below runs, failing the
            # drop and leaving the statement itself orphaned server-side. stop_statement() blocks
            # until it's genuinely terminal (a no-op if it already is, e.g. the loop ended on its
            # own because the statement failed -- passing the `Statement` object, kept fresh above,
            # rather than just its name, is what lets stop_statement() short-circuit that case
            # client-side instead of re-issuing a stop against an already-terminal statement);
            # delete_statement() then clears the now-stopped statement's server-side resources.
            print(f"{_now()} Stopping streaming statement {streaming_statement.name} ...")
            streaming_statement = conn.stop_statement(streaming_statement)
            conn.delete_statement(streaming_statement)
        if table_created:
            with conn.closing_cursor() as cursor:
                print(f"{_now()} Dropping table {table_name} ...")
                cursor.execute(f"DROP TABLE {table_name}")
    finally:
        conn.close()
