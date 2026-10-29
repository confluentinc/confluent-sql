"""Guard: every `poll_until` attempt in the driver takes the wait's `Context` and uses it.

A timed wait only keeps to its wall-clock deadline if each attempt forwards the `ctx` it is handed
to the requests it makes (`ServiceClient.request(..., ctx=ctx)`); an attempt that ignores it quietly
reintroduces the overrun by up to one request timeout. Like Go's `context.Context`, forwarding is a
convention, so this test checks it structurally across the source tree: a new wait that forgets its
`ctx` fails here, not in production. The per-wait tests prove the ctx really reaches the request;
this one keeps new waits honest.
"""

import ast
from collections.abc import Sequence
from pathlib import Path
from typing import TypeGuard

import pytest

import confluent_sql

pytestmark = pytest.mark.unit

SRC = Path(confluent_sql.__file__).parent
KNOWN_WAIT_SITES = 7
"""statement-ready, stop, Tableflow running, Tableflow removal, connector state, connector removal,
artifact removal. A new wait raises this; a scan that finds fewer means the scan went blind."""


def _is_poll_until_call(node: ast.AST) -> TypeGuard[ast.Call]:
    return isinstance(node, ast.Call) and (
        (isinstance(node.func, ast.Name) and node.func.id == "poll_until")
        or (isinstance(node.func, ast.Attribute) and node.func.attr == "poll_until")
    )


def _attempt_arg(call: ast.Call) -> ast.expr | None:
    if call.args:
        return call.args[0]
    return next((kw.value for kw in call.keywords if kw.arg == "attempt"), None)


def _uses_name(body: Sequence[ast.AST] | ast.AST, name: str) -> bool:
    nodes = body if isinstance(body, Sequence) else [body]
    return any(
        isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Load)
        for root in nodes
        for n in ast.walk(root)
    )


def ctx_violations(source: str, filename: str = "<src>") -> list[str]:
    """Problems with how each `poll_until(...)` in `source` treats its attempt's `ctx`."""
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    problems = []
    for call in filter(_is_poll_until_call, ast.walk(tree)):
        where = f"{filename}:{call.lineno}"
        attempt = _attempt_arg(call)
        if isinstance(attempt, ast.Lambda):
            params, body = attempt.args.args, attempt.body
        elif isinstance(attempt, ast.Name):
            # A nested function defined in the same enclosing function, as every real site does.
            scope = call
            while scope in parents and not isinstance(
                scope, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                scope = parents[scope]
            defs = [
                n
                for n in ast.walk(scope)
                if isinstance(n, ast.FunctionDef) and n.name == attempt.id
            ]
            if not defs:
                problems.append(f"{where}: cannot find the attempt function `{attempt.id}`")
                continue
            params, body = defs[0].args.args, defs[0].body
        else:
            problems.append(f"{where}: attempt must be a lambda or a nested function")
            continue
        if [p.arg for p in params] != ["ctx"]:
            problems.append(f"{where}: the attempt must take exactly one parameter, named `ctx`")
        elif not _uses_name(body, "ctx"):
            problems.append(f"{where}: the attempt takes `ctx` but never uses it")
    return problems


def _wait_sites() -> int:
    return sum(
        1
        for path in SRC.glob("*.py")
        if path.name != "polling.py"
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if _is_poll_until_call(node)
    )


class TestChecker:
    """The scan itself must be able to fail, or the real check below proves nothing."""

    GOOD = [
        "def f():\n    def poll(ctx):\n        return g(ctx=ctx)\n    return poll_until(poll)\n",
        "def f():\n    return poll_until(lambda ctx: g(ctx))\n",
    ]
    BAD = {
        "no-param": "def f():\n    def poll():\n        return g()\n    return poll_until(poll)\n",
        "lambda-no-param": "def f():\n    return poll_until(lambda: g())\n",
        "ignored": (
            "def f():\n    def poll(ctx):\n        return g()\n    return poll_until(poll)\n"
        ),
        "lambda-ignored": "def f():\n    return poll_until(lambda ctx: g())\n",
        "wrong-name": "def f():\n    return poll_until(lambda context: g(context))\n",
        "extra-param": "def f():\n    return poll_until(lambda ctx, x: g(ctx))\n",
    }

    @pytest.mark.parametrize("source", GOOD)
    def test_accepts_an_attempt_that_uses_its_ctx(self, source):
        assert ctx_violations(source) == []

    @pytest.mark.parametrize("source", BAD.values(), ids=BAD.keys())
    def test_flags_an_attempt_that_drops_its_ctx(self, source):
        assert len(ctx_violations(source)) == 1


class TestDriverSource:
    def test_the_scan_still_sees_every_known_wait(self):
        assert _wait_sites() >= KNOWN_WAIT_SITES

    def test_every_poll_until_attempt_takes_and_uses_its_ctx(self):
        problems = [
            problem
            for path in sorted(SRC.glob("*.py"))
            if path.name != "polling.py"
            for problem in ctx_violations(path.read_text(encoding="utf-8"), path.name)
        ]

        assert problems == []
