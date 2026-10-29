"""Unit tests for the `connect()` argument value objects."""

import dataclasses

import pytest

from confluent_sql import DEFAULT_TIMEOUTS, InterfaceError, Timeouts

pytestmark = pytest.mark.unit


class TestTimeouts:
    def test_defaults(self):
        assert Timeouts() == DEFAULT_TIMEOUTS
        assert DEFAULT_TIMEOUTS.httpx_request_timeout_secs == 10.0
        assert DEFAULT_TIMEOUTS.statement_results_response_wallclock_secs == 30.0

    def test_override_one_field_with_replace(self):
        assert dataclasses.replace(DEFAULT_TIMEOUTS, httpx_request_timeout_secs=5) == Timeouts(
            httpx_request_timeout_secs=5, statement_results_response_wallclock_secs=30.0
        )

    def test_is_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            DEFAULT_TIMEOUTS.httpx_request_timeout_secs = 1  # type: ignore[misc]

    @pytest.mark.parametrize(
        "field", ["httpx_request_timeout_secs", "statement_results_response_wallclock_secs"]
    )
    @pytest.mark.parametrize("bad", [0, -1, -0.5])
    def test_rejects_non_positive(self, field, bad):
        with pytest.raises(InterfaceError, match=rf"Timeouts\.{field} must be positive, got {bad}"):
            Timeouts(**{field: bad})

    @pytest.mark.parametrize(
        "field", ["httpx_request_timeout_secs", "statement_results_response_wallclock_secs"]
    )
    @pytest.mark.parametrize(
        ("bad", "type_name"),
        [("5", "str"), ([5], "list"), ({"secs": 5}, "dict"), (True, "bool"), (False, "bool")],
    )
    def test_rejects_non_numeric(self, field, bad, type_name):
        with pytest.raises(
            InterfaceError, match=rf"Timeouts\.{field} must be a number, got {type_name}"
        ):
            Timeouts(**{field: bad})


class TestTimeoutsWindowWarning:
    """A statement-results window no longer than one HTTP timeout means the first timed-out page is
    raised without any retry (the first timeout alone uses up the window). That can be deliberate,
    but is most often `httpx_request_timeout_secs` raised without the window, so it is flagged, not
    rejected."""

    @pytest.mark.parametrize(
        ("httpx_request_timeout_secs", "statement_results_response_wallclock_secs"),
        [(10, 5), (10, 10), (60.0, 30.0)],
        ids=["window-shorter", "window-equal", "http-raised-alone"],
    )
    def test_warns_when_the_window_cannot_fit_a_retry(
        self, httpx_request_timeout_secs, statement_results_response_wallclock_secs
    ):
        with pytest.warns(
            UserWarning,
            match=rf"statement_results_response_wallclock_secs={statement_results_response_wallclock_secs}.*httpx_request_timeout_secs={httpx_request_timeout_secs}",
        ):
            timeouts = Timeouts(
                httpx_request_timeout_secs=httpx_request_timeout_secs,
                statement_results_response_wallclock_secs=statement_results_response_wallclock_secs,
            )

        # Flagged, not rejected: it is a legitimate way to say "no timeout tolerance".
        assert (
            timeouts.statement_results_response_wallclock_secs
            == statement_results_response_wallclock_secs
        )

    def test_warning_points_at_the_caller(self):
        with pytest.warns(UserWarning) as record:
            Timeouts(httpx_request_timeout_secs=10, statement_results_response_wallclock_secs=5)

        assert record[0].filename == __file__

    @pytest.mark.parametrize(
        "timeouts_kwargs",
        [
            {},
            {"httpx_request_timeout_secs": 5},
            {"statement_results_response_wallclock_secs": 60},
            {"httpx_request_timeout_secs": 10, "statement_results_response_wallclock_secs": 10.5},
        ],
        ids=["defaults", "shorter-http", "longer-window", "window-just-fits-a-retry"],
    )
    def test_no_warning_when_the_window_exceeds_one_http_timeout(self, timeouts_kwargs, recwarn):
        Timeouts(**timeouts_kwargs)

        assert not recwarn.list
