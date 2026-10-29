import logging
from unittest.mock import Mock

import httpx
import pytest

from confluent_sql import retry
from confluent_sql.context import BACKGROUND
from confluent_sql.retry import (
    DEFAULT_RETRYABLE_EXCEPTIONS,
    DEFAULT_RETRYABLE_STATUS_CODES,
    call_with_retries,
)


@pytest.fixture()
def no_sleep(mocker):
    """Patch away real sleeping so retry tests run instantly, while still letting call counts and
    sleep durations be asserted."""
    return mocker.patch.object(retry.time, "sleep")


@pytest.fixture()
def no_jitter(mocker):
    """Pin jitter to 1.0 so sleep durations are deterministic."""
    return mocker.patch.object(retry.random, "uniform", return_value=1.0)


class _FakeClock:
    """A monotonic clock that only advances when sleep() is called."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, secs: float) -> None:
        self.slept.append(secs)
        self.now += secs


@pytest.fixture()
def clock(mocker, no_jitter) -> _FakeClock:
    """Fake time with jitter pinned, so backoff is exactly 0.1s, 0.2s, 0.4s."""
    fake = _FakeClock()
    mocker.patch.object(retry.time, "monotonic", fake.monotonic)
    mocker.patch.object(retry.time, "sleep", fake.sleep)
    return fake


@pytest.mark.unit
class TestRetriesStopAtTheCtxDeadline:
    """Given a `ctx`, a retry happens only if the deadline outlasts the backoff sleep, so no sleep
    runs past it and no attempt is begun that has no time left; the last attempt's own exception is
    raised, exactly as when `max_retries` runs out."""

    def test_an_expired_ctx_allows_the_one_attempt_and_no_retry(self, clock):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with pytest.raises(httpx.ReadError):
            call_with_retries(func, ctx=BACKGROUND.with_timeout(-1.0))

        func.assert_called_once()
        assert clock.slept == []

    def test_no_retry_when_the_deadline_is_nearer_than_the_first_backoff(self, clock):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with pytest.raises(httpx.ReadError):
            call_with_retries(func, ctx=BACKGROUND.with_timeout(0.05))

        func.assert_called_once()
        assert clock.slept == []

    def test_retries_continue_while_the_deadline_outlasts_each_sleep_then_stop(self, clock):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with pytest.raises(httpx.ReadError):
            call_with_retries(func, ctx=BACKGROUND.with_timeout(0.35))

        # 0.35s of budget: sleep 0.1 (0.25 left), sleep 0.2 (0.05 left), then 0.4 would overrun.
        assert func.call_count == 3
        assert clock.slept == [0.1, 0.2]

    def test_a_generous_ctx_leaves_max_retries_as_the_bound(self, clock):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with pytest.raises(httpx.ReadError):
            call_with_retries(func, max_retries=3, ctx=BACKGROUND.with_timeout(600.0))

        assert func.call_count == 4

    def test_succeeds_on_a_retry_within_the_deadline(self, clock):
        func = Mock(side_effect=[httpx.ReadError("blip"), "ok"])

        assert call_with_retries(func, ctx=BACKGROUND.with_timeout(5.0)) == "ok"

    def test_logs_that_the_deadline_stopped_the_retry(self, clock, caplog):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with (
            caplog.at_level(logging.INFO, logger="confluent_sql.retry"),
            pytest.raises(httpx.ReadError),
        ):
            call_with_retries(func, ctx=BACKGROUND.with_timeout(0.05))

        assert any("deadline" in record.getMessage() for record in caplog.records)


@pytest.mark.unit
class TestCallWithRetries:
    def test_succeeds_without_retrying(self, no_sleep):
        func = Mock(return_value="ok")

        result = call_with_retries(func, "a", b="c")

        assert result == "ok"
        func.assert_called_once_with("a", b="c")
        no_sleep.assert_not_called()

    def test_retries_and_eventually_succeeds(self, no_sleep, no_jitter):
        func = Mock(
            side_effect=[
                httpx.ReadError("boom"),
                httpx.ReadError("boom again"),
                "ok",
            ]
        )

        result = call_with_retries(func, "a", b="c")

        assert result == "ok"
        assert func.call_count == 3
        for call in func.call_args_list:
            assert call.args == ("a",)
            assert call.kwargs == {"b": "c"}

    def test_exhausts_retries_and_reraises_original_exception(self, no_sleep, no_jitter):
        original = httpx.ReadError("boom")
        func = Mock(side_effect=[original, httpx.ReadError("2"), httpx.ReadError("3"), original])

        with pytest.raises(httpx.ReadError) as exc_info:
            call_with_retries(func, max_retries=3)

        assert exc_info.value is original
        assert func.call_count == 4

    def test_logs_one_info_record_per_retry_naming_attempt_number_and_exception_message(
        self, no_sleep, no_jitter, caplog
    ):
        func = Mock(
            side_effect=[
                httpx.ReadError("Connection reset by peer"),
                httpx.RemoteProtocolError("Server disconnected without sending a response"),
                "ok",
            ]
        )

        with caplog.at_level(logging.INFO, logger=retry.logger.name):
            call_with_retries(func, max_retries=3)

        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert len(info_records) == 2
        assert "attempt 1/3" in info_records[0].message
        assert '"Connection reset by peer"' in info_records[0].message
        assert "attempt 2/3" in info_records[1].message
        assert '"Server disconnected without sending a response"' in info_records[1].message

    def test_non_retryable_exception_propagates_without_retry(self, no_sleep):
        func = Mock(side_effect=ValueError("not retryable"))

        with pytest.raises(ValueError, match="not retryable"):
            call_with_retries(func)

        func.assert_called_once()
        no_sleep.assert_not_called()

    def test_max_retries_zero_means_single_call_and_immediate_reraise(self, no_sleep):
        func = Mock(side_effect=httpx.ReadError("boom"))

        with pytest.raises(httpx.ReadError):
            call_with_retries(func, max_retries=0)

        func.assert_called_once()
        no_sleep.assert_not_called()

    def test_custom_exceptions_tuple_overrides_default_set(self, no_sleep, no_jitter):
        func = Mock(side_effect=[ValueError("custom"), "ok"])

        result = call_with_retries(func, exceptions=(ValueError,))

        assert result == "ok"
        assert func.call_count == 2

    def test_backoff_grows_and_is_jittered(self, mocker, no_sleep):
        uniform = mocker.patch.object(retry.random, "uniform", side_effect=[1.0, 0.75, 1.25])
        func = Mock(
            side_effect=[
                httpx.ReadError("1"),
                httpx.ReadError("2"),
                httpx.ReadError("3"),
                "ok",
            ]
        )

        call_with_retries(func, max_retries=3)

        slept = [call.args[0] for call in no_sleep.call_args_list]
        assert len(slept) == 3
        # Each successive base delay grows; jitter draws (1.0, 0.75, 1.25) scale it further.
        assert slept[0] < slept[1] < slept[2]
        assert uniform.call_count == 3

    def test_default_retryable_exceptions(self):
        assert httpx.NetworkError in DEFAULT_RETRYABLE_EXCEPTIONS
        assert httpx.RemoteProtocolError in DEFAULT_RETRYABLE_EXCEPTIONS
        assert httpx.TimeoutException not in DEFAULT_RETRYABLE_EXCEPTIONS

    def test_default_exceptions_do_not_retry_timeout(self, no_sleep):
        """Tuple membership alone (test_default_retryable_exceptions above) doesn't prove
        httpx.TimeoutException is never retried -- it would still be caught if it were a
        subclass of one of the default exceptions. Assert the actual behavior instead: a timeout
        propagates on the first call, with no retry and no sleep."""
        func = Mock(side_effect=httpx.TimeoutException("timed out"))

        with pytest.raises(httpx.TimeoutException):
            call_with_retries(func)

        func.assert_called_once()
        no_sleep.assert_not_called()

    def test_default_retryable_status_codes(self):
        assert frozenset({429, 500, 502, 503, 504}) == DEFAULT_RETRYABLE_STATUS_CODES
        assert 408 not in DEFAULT_RETRYABLE_STATUS_CODES
        assert 200 not in DEFAULT_RETRYABLE_STATUS_CODES
        assert 404 not in DEFAULT_RETRYABLE_STATUS_CODES
        assert 400 not in DEFAULT_RETRYABLE_STATUS_CODES
