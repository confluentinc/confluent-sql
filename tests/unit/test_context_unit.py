"""Unit tests for `Context`: a Go-style carrier of a wall-clock deadline, passed down explicitly."""

import pytest

from confluent_sql import context as context_module
from confluent_sql.context import BACKGROUND, Context

pytestmark = pytest.mark.unit


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture()
def clock(mocker) -> _Clock:
    fake = _Clock()
    mocker.patch.object(context_module.time, "monotonic", fake.monotonic)
    return fake


class TestContext:
    def test_background_has_no_deadline(self):
        assert BACKGROUND.remaining_secs() is None
        assert Context.background() is BACKGROUND

    def test_with_timeout_sets_a_deadline_that_counts_down(self, clock):
        ctx = BACKGROUND.with_timeout(10.0)

        assert ctx.remaining_secs() == 10.0
        clock.now += 4.0
        assert ctx.remaining_secs() == 6.0

    def test_remaining_goes_negative_once_expired(self, clock):
        ctx = BACKGROUND.with_timeout(1.0)
        clock.now += 3.0

        assert ctx.remaining_secs() == -2.0

    @pytest.mark.parametrize(
        ("parent_secs", "child_secs", "expected"),
        [(10.0, 3.0, 3.0), (2.0, 50.0, 2.0)],
        ids=["child-tighter", "parent-tighter"],
    )
    def test_a_child_can_only_tighten_its_parents_deadline(
        self, clock, parent_secs, child_secs, expected
    ):
        child = BACKGROUND.with_timeout(parent_secs).with_timeout(child_secs)

        assert child.remaining_secs() == expected

    def test_with_timeout_leaves_the_parent_untouched(self, clock):
        parent = BACKGROUND.with_timeout(10.0)
        parent.with_timeout(1.0)

        assert parent.remaining_secs() == 10.0
