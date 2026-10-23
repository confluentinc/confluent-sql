import pytest

from confluent_sql import polling
from confluent_sql.exceptions import OperationalError, RequestTimeoutError
from confluent_sql.polling import poll_until, sleep_with_backoff


class _FakeClock:
    """A monotonic clock that only advances when sleep() is called, so backoff pacing can be
    exercised deterministically without real time passing."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, secs: float) -> None:
        self.slept.append(secs)
        self.now += secs


@pytest.fixture()
def fake_clock(mocker) -> _FakeClock:
    """Install a fake clock and disable jitter (uniform -> 1.0) inside the polling module."""
    clock = _FakeClock()
    mocker.patch.object(polling.time, "monotonic", clock.monotonic)
    mocker.patch.object(polling.time, "sleep", clock.sleep)
    mocker.patch.object(polling.random, "uniform", return_value=1.0)
    return clock


@pytest.mark.unit
class TestSleepWithBackoff:
    """Tests for the shared backoff pacing generator."""

    def test_sleeps_grow_geometrically_then_clamp_to_timeout(self, fake_clock: _FakeClock):
        """Each retry sleeps base * growth**n (jitter pinned to 1.0); the final sleep is clamped to
        the remaining budget so the cumulative wait equals -- and never exceeds -- the timeout."""
        yields = list(sleep_with_backoff(10.0))

        # base=1.0, growth=1.5: 1.0, 1.5, 2.25, 3.375 -> cumulative 8.125; the next full step
        # (5.0625) would overshoot, so it is clamped to the remaining 1.875.
        assert fake_clock.slept == [1.0, 1.5, 2.25, 3.375, 1.875]
        assert sum(fake_clock.slept) == 10.0
        assert len(yields) == len(fake_clock.slept) == 5

    def test_delay_is_capped(self, fake_clock: _FakeClock):
        """The per-retry sleep never exceeds the 30s ceiling, even after many retries, and the
        total wait still lands exactly on the timeout (final sleep clamped to the remainder)."""
        list(sleep_with_backoff(500.0))

        assert max(fake_clock.slept) == 30.0
        # The ceiling is held on interior retries; only the last sleep is clamped down.
        assert fake_clock.slept[-2] == 30.0
        assert sum(fake_clock.slept) == pytest.approx(500.0)
        assert fake_clock.slept[-1] < 30.0

    def test_small_timeout_does_not_overshoot(self, fake_clock: _FakeClock):
        """A sub-base-delay timeout sleeps only the remaining budget and yields once, rather than
        sleeping a full base delay (up to 1.25s with jitter) past the requested deadline."""
        yields = list(sleep_with_backoff(0.1))

        assert fake_clock.slept == [0.1]
        assert len(yields) == 1

    def test_zero_timeout_yields_nothing(self, fake_clock: _FakeClock):
        """A non-positive timeout produces no retries and never sleeps."""
        assert list(sleep_with_backoff(0.0)) == []
        assert fake_clock.slept == []

    def test_jitter_never_exceeds_ceiling(self, mocker):
        """Even at maximum upward jitter, no individual sleep exceeds the 30s ceiling -- the cap
        applies to the jittered sleep, not merely to the pre-jitter base delay."""
        clock = _FakeClock()
        mocker.patch.object(polling.time, "monotonic", clock.monotonic)
        mocker.patch.object(polling.time, "sleep", clock.sleep)
        mocker.patch.object(polling.random, "uniform", return_value=1.25)

        list(sleep_with_backoff(1000.0))

        # Pre-jitter delay caps at 30s; 30 * 1.25 would be 37.5s without a post-jitter clamp.
        assert max(clock.slept) == 30.0

    def test_started_at_counts_prior_elapsed_against_budget(self, fake_clock: _FakeClock):
        """A caller that made its own first attempt can pass started_at so the time already spent
        (e.g. the initial network poll) counts against the timeout, keeping total wall time from
        that origin within the timeout."""
        # Simulate 3s already consumed before pacing begins.
        fake_clock.now = 3.0

        list(sleep_with_backoff(10.0, started_at=0.0))

        # Budget is 10s measured from t=0; 3s already gone leaves ~7s of sleeping.
        assert sum(fake_clock.slept) == pytest.approx(7.0)
        assert fake_clock.now == pytest.approx(10.0)

    def test_caller_work_between_yields_is_charged_against_budget(self, fake_clock: _FakeClock):
        """Time the caller spends between yields (e.g. a network GET) counts against the timeout,
        so a slow caller ends pacing sooner -- the budget is shared across sleeps and caller work,
        not reset per retry. The generator cannot bound the caller's own in-flight work, though:
        total wall time can still exceed the timeout by the duration of work already underway."""
        yields = 0
        for _ in sleep_with_backoff(10.0):
            yields += 1
            fake_clock.now += 4.0  # simulate a 4s caller GET in each loop body

        # sleep 1.0 (t=1) + work (t=5); sleep 1.5 (t=6.5) + work (t=10.5); next check 10.5 > 10.
        assert yields == 2
        assert fake_clock.slept == [1.0, 1.5]
        # Caller work pushed total wall time past the 10s timeout -- the helper bounds its sleeping,
        # not the caller's GETs.
        assert fake_clock.now == pytest.approx(10.5)

    def test_jitter_band_applied(self, mocker):
        """Each sleep is scaled by a ±25% jitter draw."""
        clock = _FakeClock()
        mocker.patch.object(polling.time, "monotonic", clock.monotonic)
        mocker.patch.object(polling.time, "sleep", clock.sleep)
        uniform = mocker.patch.object(polling.random, "uniform", return_value=1.0)

        list(sleep_with_backoff(2.0))

        uniform.assert_called_with(0.75, 1.25)


@pytest.mark.unit
class TestPollUntil:
    """poll_until owns the whole wait loop: pacing, deadline, slow-poll tolerance (#236)."""

    @staticmethod
    def _deadline_error() -> OperationalError:
        return OperationalError("gave up")

    def test_returns_first_attempt_result_without_sleeping(self, fake_clock):
        result = poll_until(lambda: "done", timeout=10, on_timeout=self._deadline_error)

        assert result == "done"
        assert fake_clock.slept == []

    def test_not_immediate_sleeps_before_first_attempt(self, fake_clock):
        attempts_at: list[float] = []

        def attempt() -> str:
            attempts_at.append(fake_clock.now)
            return "done"

        poll_until(attempt, timeout=10, immediate=False, on_timeout=self._deadline_error)

        assert attempts_at == [1.0]

    def test_polls_until_attempt_yields_a_result(self, fake_clock):
        results = iter([None, None, "done"])

        result = poll_until(lambda: next(results), timeout=60, on_timeout=self._deadline_error)

        assert result == "done"
        assert len(fake_clock.slept) == 2

    def test_slow_poll_is_treated_as_not_done_yet(self, fake_clock):
        outcomes = iter([RequestTimeoutError("slow"), None, RequestTimeoutError("slow"), "done"])

        def attempt() -> str | None:
            outcome = next(outcomes)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        assert poll_until(attempt, timeout=60, on_timeout=self._deadline_error) == "done"

    def test_deadline_raises_on_timeout_error_chained_to_last_slow_poll(self, fake_clock):
        slow = RequestTimeoutError("slow")

        def attempt() -> str:
            raise slow

        with pytest.raises(OperationalError, match="gave up") as exc_info:
            poll_until(attempt, timeout=5, on_timeout=self._deadline_error)

        assert not isinstance(exc_info.value, RequestTimeoutError)
        assert exc_info.value.__cause__ is slow

    def test_deadline_without_slow_polls_has_no_cause(self, fake_clock):
        with pytest.raises(OperationalError, match="gave up") as exc_info:
            poll_until(lambda: None, timeout=5, on_timeout=self._deadline_error)

        assert exc_info.value.__cause__ is None

    def test_other_errors_propagate(self, fake_clock):
        def attempt() -> str:
            raise OperationalError("hard failure")

        with pytest.raises(OperationalError, match="hard failure"):
            poll_until(attempt, timeout=60, on_timeout=self._deadline_error)

    def test_slow_first_attempt_is_charged_against_the_budget(self, fake_clock):
        calls = 0

        def attempt() -> None:
            nonlocal calls
            calls += 1
            fake_clock.now += 10  # the attempt itself consumes the whole budget

        with pytest.raises(OperationalError, match="gave up"):
            poll_until(attempt, timeout=10, on_timeout=self._deadline_error)

        assert calls == 1
