import logging
import threading
from pathlib import Path

import httpx
import numpy
import pandas
import pytest
import stamina
import yaml
from pydantic import ValidationError
from starlette.status import HTTP_400_BAD_REQUEST

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context, from_profile, record_history
from tiled.client.logger import hide_logs, show_logs
from tiled.client.utils import retry_context
from tiled.profiles import load_profiles, paths
from tiled.queries import Key
from tiled.server.app import build_app

from .utils import fail_with_status_code

tree = MapAdapter({})


def test_configurable_timeout():
    with Context.from_app(build_app(tree), timeout=httpx.Timeout(17)) as context:
        assert context.http_client.timeout.connect == 17
        assert context.http_client.timeout.read == 17


def test_configurable_max_connections():
    "max_connections is reflected in the semaphore on the Context."
    with Context.from_app(build_app(tree), max_connections=3) as context:
        assert context._concurrent_request_semaphore._value == 3


def test_client_version_check(caplog):
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)

        # Too-old user agent should generate a 400.
        context.http_client.headers["user-agent"] = "python-tiled/0.1.0a77"
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            list(client)

        # Gibberish user agent should generate a warning and log entry.
        context.http_client.headers["user-agent"] = "python-tiled/gibberish"
        caplog.set_level(logging.WARNING)
        with pytest.warns(UserWarning, match=r"gibberish"):
            list(client)

        _, LOG_LEVEL, LOG_MESSAGE = range(3)
        logged_warnings = tuple(
            entry[LOG_MESSAGE]
            for entry in caplog.record_tuples
            if entry[LOG_LEVEL] == logging.WARNING
        )
        assert len(logged_warnings) > 0
        assert any("gibberish" in message for message in logged_warnings)


def test_direct(tmpdir):
    profile_content = {
        "test": {
            "structure_clients": "dask",
            "direct": {
                "trees": [
                    {"path": "/", "tree": "tiled.examples.generated_minimal:tree"}
                ]
            },
        }
    }
    with open(tmpdir / "example.yml", "w") as file:
        file.write(yaml.dump(profile_content))
    profile_dir = Path(tmpdir)
    try:
        paths.append(profile_dir)
        load_profiles.cache_clear()
        from_profile("test")
    finally:
        paths.remove(profile_dir)


def test_direct_config_error(tmpdir):
    profile_content = {
        "test": {
            "direct": {
                # Intentional config mistake!
                # Value of trees must be a list.
                "trees": {"path": "/", "tree": "tiled.examples.generated_minimal:tree"}
            }
        }
    }
    with open(tmpdir / "example.yml", "w") as file:
        file.write(yaml.dump(profile_content))
    profile_dir = Path(tmpdir)
    try:
        paths.append(profile_dir)
        load_profiles.cache_clear()
        with pytest.raises(ValidationError):
            from_profile("test")
    finally:
        paths.remove(profile_dir)


def test_jump_down_tree():
    tree = MapAdapter({}, metadata={"number": 1})
    for number, letter in enumerate(list("abcde"), start=2):
        tree = MapAdapter({letter: tree}, metadata={"number": number})
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert (
        client["e"]["d"]["c"]["b"]["a"].metadata["number"]
        == client["e", "d", "c", "b", "a"].metadata["number"]
        == 1
    )
    assert (
        client["e"]["d"]["c"]["b"].metadata["number"]
        == client["e", "d", "c", "b"].metadata["number"]
        == 2
    )
    assert (
        client["e"]["d"]["c"].metadata["number"]
        == client["e", "d", "c"].metadata["number"]
        == 3
    )
    assert (
        client["e"]["d"].metadata["number"] == client["e", "d"].metadata["number"] == 4
    )

    assert client["e"]["d", "c", "b"]["a"].metadata["number"] == 1
    assert client["e"]["d", "c", "b", "a"].metadata["number"] == 1
    assert client["e", "d", "c", "b"]["a"].metadata["number"] == 1
    assert (
        client.search(Key("number") == 5)["e", "d", "c", "b", "a"].metadata["number"]
        == 1
    )
    assert (
        client["e"].search(Key("number") == 4)["d", "c", "b", "a"].metadata["number"]
        == 1
    )

    # Check that a reasonable KeyError is raised.
    # Notice that we do not binary search to find _exactly_ where the problem is.
    with pytest.raises(KeyError) as exc_info:
        client["e", "d", "c", "b"]["X"]
    assert exc_info.value.args[0] == "X"
    with pytest.raises(KeyError) as exc_info:
        client["e", "d", "c", "b", "X"]
    assert exc_info.value.args[0] == ("e", "d", "c", "b", "X")
    with pytest.raises(KeyError) as exc_info:
        client["e", "d", "X", "b", "a"]
    assert exc_info.value.args[0] == ("e", "d", "X", "b", "a")

    # Check that jumping raises if a key along the path is not in the search
    # resuts.
    with pytest.raises(KeyError) as exc_info:
        client.search(Key("number") == 4)["e"]
    assert exc_info.value.args[0] == "e"
    with pytest.raises(KeyError) as exc_info:
        client.search(Key("number") == 4)["e", "d", "c", "b", "a"]
    assert exc_info.value.args[0] == "e"
    with pytest.raises(KeyError) as exc_info:
        client["e"].search(Key("number") == 3)["d"]
    assert exc_info.value.args[0] == "d"
    with pytest.raises(KeyError) as exc_info:
        client["e"].search(Key("number") == 3)["d", "c", "b", "a"]
    assert exc_info.value.args[0] == "d"

    with record_history() as h:
        client["e", "d", "c", "b", "a"]
    assert len(h.requests) == 1

    with record_history() as h:
        client["e"]["d"]["c"]["b"]["a"]
    assert len(h.requests) == 5


def test_no_stamina_retry_scheduled_messages(caplog):
    """stamina's default 'stamina.retry_scheduled' WARNING must never appear.

    Tiled removes the default stamina hook so that no global retry noise is emitted,
    regardless of show_logs()/hide_logs() state.
    """
    stamina.set_active(True)
    try:
        n = 0
        with caplog.at_level(logging.DEBUG, logger="stamina"):
            for attempt in stamina.retry_context(on=Exception, attempts=2, timeout=10):
                with attempt:
                    n += 1
                    if n < 2:
                        raise ValueError("transient error")

        stamina_messages = [r for r in caplog.records if r.name == "stamina"]
        assert (
            len(stamina_messages) == 0
        ), "stamina's default hook should be disabled — no 'stamina.retry_scheduled' messages"
    finally:
        stamina.set_active(False)


def test_tiled_retry_logging_visible_after_show_logs(caplog):
    """After show_logs(), tiled retry messages appear on the tiled.client logger."""
    show_logs()

    stamina.set_active(True)
    try:
        n = 0
        with caplog.at_level(logging.DEBUG, logger="tiled.client"):
            for attempt in retry_context():
                with attempt:
                    n += 1
                    if n < 2:
                        raise httpx.ReadTimeout("simulated timeout")

        tiled_messages = [r for r in caplog.records if r.name == "tiled.client"]
        assert (
            len(tiled_messages) >= 1
        ), "Tiled retries should emit DEBUG messages on tiled.client logger"
        assert all(r.levelno == logging.DEBUG for r in tiled_messages)
    finally:
        stamina.set_active(False)
        hide_logs()


def test_tiled_retry_logging_silent_by_default(caplog):
    """By default (hide_logs), tiled retry messages are not visible."""
    hide_logs()

    stamina.set_active(True)
    try:
        n = 0
        # Capture at DEBUG to ensure we'd see it if it leaked through.
        with caplog.at_level(logging.DEBUG):
            for attempt in retry_context():
                with attempt:
                    n += 1
                    if n < 2:
                        raise httpx.ReadTimeout("simulated timeout")

        # No tiled.client messages should appear (logger is at WARNING).
        tiled_messages = [r for r in caplog.records if r.name == "tiled.client"]
        assert (
            len(tiled_messages) == 0
        ), "Tiled retry messages should be silent by default"
    finally:
        stamina.set_active(False)


class TrackingSemaphore:
    "Drop-in replacement for `threading.Semaphore` that also records peak concurrent holders"

    def __init__(self, value):
        self._sem = threading.Semaphore(value)
        self._lock = threading.Lock()
        self.current = 0
        self.peak = 0

    def acquire(self, *args, **kwargs):
        self._sem.acquire(*args, **kwargs)
        with self._lock:
            self.current += 1
            if self.current > self.peak:
                self.peak = self.current

    def release(self):
        with self._lock:
            self.current -= 1
        self._sem.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()


def test_semaphore_limits_concurrent_array_fetches():
    """When dask computes a chunked array the semaphore must cap concurrent fetches.

    We use max_connections=2 and an array split across 10 chunks so that dask
    would fire all requests at once without the semaphore.
    """
    MAX_CONNECTIONS = 2

    import dask.array as da

    arr = da.zeros((10, 300, 400), chunks=(1, 300, 400), dtype="float32")
    tree = MapAdapter({"data": ArrayAdapter.from_array(arr)})
    app = build_app(tree)

    with Context.from_app(app, max_connections=MAX_CONNECTIONS) as context:
        sem = TrackingSemaphore(MAX_CONNECTIONS)
        context._concurrent_request_semaphore = sem

        client = from_context(context, structure_clients="dask")["data"]
        client.read().compute()

    assert sem.peak <= MAX_CONNECTIONS
    # Sanity: 10 chunks > MAX_CONNECTIONS, so the cap had something to constrain.
    assert sem.peak > 0


def test_semaphore_limits_concurrent_partition_fetches():
    "When dask computes a partitioned dataframe the semaphore must cap concurrent fetches"

    MAX_CONNECTIONS = 2
    N_PARTITIONS = 8  # well above the cap

    df = pandas.DataFrame({"x": numpy.arange(N_PARTITIONS * 10, dtype="float64")})
    tree = MapAdapter(
        {"data": DataFrameAdapter.from_pandas(df, npartitions=N_PARTITIONS)}
    )
    app = build_app(tree)

    with Context.from_app(app, max_connections=MAX_CONNECTIONS) as context:
        sem = TrackingSemaphore(MAX_CONNECTIONS)
        context._concurrent_request_semaphore = sem

        client = from_context(context, structure_clients="dask")["data"]
        client.read().compute()

    assert sem.peak <= MAX_CONNECTIONS
    assert sem.peak > 0


# --- Progress bar tests ---


def test_tracking_progress_sets_and_clears_state():
    """tracking_progress sets _progress_state during the context and clears it after."""
    from unittest.mock import patch

    from tiled.client.utils import tracking_progress

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        with patch("tiled.client.utils._is_interactive", return_value=True):
            with tracking_progress(context, total=5):
                assert context._progress_state is not None
                state = context._progress_state
                assert state._task_id is not None

        # After exit, state is cleared
        assert context._progress_state is None


def test_tracking_progress_noop_when_show_progress_false():
    """tracking_progress is a no-op when show_progress is False."""
    from tiled.client.utils import tracking_progress

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=False) as context:
        with tracking_progress(context, total=10):
            assert context._progress_state is None


def test_tracking_progress_nesting_is_noop():
    """Inner tracking_progress defers to outer (no nested bars)."""
    from unittest.mock import patch

    from tiled.client.utils import tracking_progress

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        with patch("tiled.client.utils._is_interactive", return_value=True):
            with tracking_progress(context, total=10):
                outer_state = context._progress_state
                assert outer_state is not None

                # Inner tracking_progress should NOT overwrite
                with tracking_progress(context, total=5):
                    assert context._progress_state is outer_state

            # After outer exits, state is cleared
            assert context._progress_state is None


def test_show_progress_from_env_var(monkeypatch):
    """TILED_SHOW_PROGRESS env var controls context.show_progress."""
    tree_local = MapAdapter({})
    app = build_app(tree_local)

    # Default (no env var) is True
    monkeypatch.delenv("TILED_SHOW_PROGRESS", raising=False)
    with Context.from_app(app) as context:
        assert context.show_progress is True

    monkeypatch.setenv("TILED_SHOW_PROGRESS", "1")
    with Context.from_app(app) as context:
        assert context.show_progress is True

    monkeypatch.setenv("TILED_SHOW_PROGRESS", "0")
    with Context.from_app(app) as context:
        assert context.show_progress is False

    monkeypatch.setenv("TILED_SHOW_PROGRESS", "false")
    with Context.from_app(app) as context:
        assert context.show_progress is False

    monkeypatch.setenv("TILED_SHOW_PROGRESS", "no")
    with Context.from_app(app) as context:
        assert context.show_progress is False


def test_show_progress_explicit_overrides_env(monkeypatch):
    """Explicit show_progress=True/False overrides the env var."""
    monkeypatch.setenv("TILED_SHOW_PROGRESS", "0")
    tree_local = MapAdapter({})
    app = build_app(tree_local)
    with Context.from_app(app, show_progress=True) as context:
        assert context.show_progress is True

    monkeypatch.setenv("TILED_SHOW_PROGRESS", "1")
    with Context.from_app(app, show_progress=False) as context:
        assert context.show_progress is False


# --- Retry indicator tests ---


def test_progress_state_show_hide_retrying():
    """_ProgressState.show_retrying/hide_retrying toggle state and update Live."""
    from unittest.mock import patch

    from tiled.client.utils import tracking_progress

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        with patch("tiled.client.utils._is_interactive", return_value=True):
            with tracking_progress(context, total=5) as state:
                assert state._retrying is False

                # Show retrying
                state.show_retrying()
                assert state._retrying is True

                # Calling again is a no-op
                state.show_retrying()
                assert state._retrying is True

                # Hide retrying
                state.hide_retrying()
                assert state._retrying is False

                # Calling hide again is a no-op
                state.hide_retrying()
                assert state._retrying is False


def test_progress_state_show_retrying_no_name_error():
    """_ProgressState.show_retrying does not raise NameError for Group import."""
    from unittest.mock import patch

    from tiled.client.utils import tracking_progress

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        with patch("tiled.client.utils._is_interactive", return_value=True):
            with tracking_progress(context, total=5) as state:
                # This previously raised NameError: name 'Group' is not defined
                state.show_retrying()
                state.hide_retrying()


def test_standalone_retry_indicator_show_hide():
    """_StandaloneRetryIndicator: plain text on non-TTY, Live spinner on TTY."""
    import sys
    from io import StringIO
    from unittest.mock import MagicMock, patch

    from tiled.client.utils import _StandaloneRetryIndicator

    # Non-TTY: plain text written once, reset() is a no-op
    indicator = _StandaloneRetryIndicator()
    assert indicator._showing is False

    fake_stderr = StringIO()
    with patch.object(sys, "stderr", fake_stderr):
        indicator.show()
        assert indicator._showing is True
        assert "Retrying" in fake_stderr.getvalue()

        # Second show() is a no-op
        fake_stderr.truncate(0)
        fake_stderr.seek(0)
        indicator.show()
        assert fake_stderr.getvalue() == ""

        indicator.reset()
        assert indicator._showing is False

        # reset() again is a no-op
        indicator.reset()
        assert indicator._showing is False

    # TTY: show() starts a Live spinner; reset() stops it
    mock_live = MagicMock()
    indicator2 = _StandaloneRetryIndicator()
    with patch("rich.live.Live", return_value=mock_live):
        tty_stderr = MagicMock()
        tty_stderr.isatty = lambda: True
        with patch.object(sys, "stderr", tty_stderr):
            indicator2.show()
            assert indicator2._live is mock_live
            mock_live.start.assert_called_once()

            # Second show() is a no-op
            indicator2.show()
            mock_live.start.assert_called_once()

            indicator2.reset()
            assert indicator2._live is None
            mock_live.stop.assert_called_once()

            # reset() again is a no-op
            indicator2.reset()
            mock_live.stop.assert_called_once()


def test_signal_retry_uses_standalone_indicator_when_no_progress():
    """_signal_retry creates standalone indicator when no progress bar is active."""
    import sys
    from io import StringIO
    from unittest.mock import patch

    from tiled.client.utils import _signal_retry, _signal_retry_resolved

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        assert context._retry_indicator is None

        fake_stderr = StringIO()
        with patch.object(sys, "stderr", fake_stderr):
            _signal_retry(context)
            assert context._retry_indicator is not None
            assert context._retry_indicator._showing is True
            assert "Retrying" in fake_stderr.getvalue()

            _signal_retry_resolved(context)
            assert context._retry_indicator is None


def test_retry_context_no_context_shows_indicator():
    """retry_context() with no context still shows the standalone retry indicator."""
    import sys
    from io import StringIO
    from unittest.mock import patch

    import stamina

    from tiled.client.utils import retry_context

    fake_stderr = StringIO()
    with patch.object(sys, "stderr", fake_stderr):
        try:
            stamina.set_active(True)
            call_count = 0
            for attempt in retry_context():  # no context
                with attempt:
                    call_count += 1
                    if call_count < 3:
                        raise httpx.ConnectError("test")
            assert "Retrying" in fake_stderr.getvalue()
        finally:
            stamina.set_active(False)


def test_signal_retry_uses_progress_state_when_available():
    """_signal_retry calls show_retrying on _progress_state if set."""
    from unittest.mock import MagicMock

    from tiled.client.utils import _signal_retry, _signal_retry_resolved

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        mock_state = MagicMock()
        context._progress_state = mock_state

        _signal_retry(context)
        mock_state.show_retrying.assert_called_once()

        _signal_retry_resolved(context)
        mock_state.hide_retrying.assert_called_once()

        context._progress_state = None


def test_signal_retry_shows_regardless_of_show_progress():
    """_signal_retry always shows indicator, even when show_progress is False."""
    import sys
    from io import StringIO
    from unittest.mock import patch

    from tiled.client.utils import _signal_retry, _signal_retry_resolved

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=False) as context:
        fake_stderr = StringIO()
        with patch.object(sys, "stderr", fake_stderr):
            _signal_retry(context)
            assert context._retry_indicator is not None
            assert context._retry_indicator._showing is True
            assert "Retrying" in fake_stderr.getvalue()
            _signal_retry_resolved(context)
            assert context._retry_indicator is None


def test_retry_context_signals_retry_indicator():
    """retry_context with a context signals the retry indicator on retry."""
    from unittest.mock import patch

    import stamina

    from tiled.client.utils import retry_context

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        with patch("tiled.client.utils._signal_retry") as mock_signal:
            try:
                stamina.set_active(True)
                call_count = 0

                for attempt in retry_context(context):
                    with attempt:
                        call_count += 1
                        if call_count < 3:
                            raise httpx.ConnectError("test")
                # _signal_retry should have been called for attempts 2 and 3
                # (retry scheduled after attempt 1 and 2 fail)
                assert mock_signal.call_count == 2
            finally:
                stamina.set_active(False)


# --- 429 Too Many Requests tests ---


def test_keyboard_interrupt_cancels_retries():
    """KeyboardInterrupt during inter-retry sleep propagates and cancels remaining retries."""
    from unittest.mock import patch

    import stamina

    from tiled.client.utils import retry_context

    attempts_made = []

    def fake_sleep(_):
        raise KeyboardInterrupt("simulated Ctrl-C")

    try:
        stamina.set_active(True)
        with patch("time.sleep", fake_sleep):
            try:
                for attempt in retry_context():
                    with attempt:
                        attempts_made.append(attempt.num)
                        raise httpx.ConnectError("refused")
            except KeyboardInterrupt:
                pass  # expected
            else:
                raise AssertionError("KeyboardInterrupt was not raised")
    finally:
        stamina.set_active(False)

    # Only 1 attempt made — Ctrl-C during the sleep before attempt 2
    assert len(attempts_made) == 1, f"Expected 1 attempt, got {len(attempts_made)}"


def test_keyboard_interrupt_cleans_up_retry_indicator():
    """KeyboardInterrupt cancels retries and cleans up the retry indicator."""
    from unittest.mock import MagicMock, patch

    import stamina

    from tiled.client.utils import retry_context

    tree = MapAdapter({"data": ArrayAdapter.from_array(numpy.zeros((10,)))})
    app = build_app(tree)

    with Context.from_app(app, show_progress=True) as context:
        mock_state = MagicMock()
        context._progress_state = mock_state

        def fake_sleep(_):
            raise KeyboardInterrupt("simulated Ctrl-C")

        try:
            stamina.set_active(True)
            with patch("time.sleep", fake_sleep):
                try:
                    for attempt in retry_context(context):
                        with attempt:
                            raise httpx.ConnectError("refused")
                except KeyboardInterrupt:
                    pass
        finally:
            stamina.set_active(False)
            context._progress_state = None

        # hide_retrying should have been called on Ctrl-C
        mock_state.hide_retrying.assert_called()


def test_should_retry_returns_retry_after_for_429():
    """should_retry returns the Retry-After value as a float for 429 responses."""
    from tiled.client.utils import should_retry

    request = httpx.Request("GET", "http://example.com/test")
    response = httpx.Response(429, headers={"Retry-After": "2.5"}, request=request)
    exc = httpx.HTTPStatusError("rate limited", request=request, response=response)
    result = should_retry(exc)
    assert result == 2.5


def test_should_retry_returns_true_for_429_without_header():
    """should_retry returns True (use default backoff) for 429 without Retry-After."""
    from tiled.client.utils import should_retry

    request = httpx.Request("GET", "http://example.com/test")
    response = httpx.Response(429, request=request)
    exc = httpx.HTTPStatusError("rate limited", request=request, response=response)
    result = should_retry(exc)
    assert result is True


def test_should_retry_does_not_retry_other_4xx():
    """should_retry returns False for non-429 client errors."""
    from tiled.client.utils import should_retry

    request = httpx.Request("GET", "http://example.com/test")
    response = httpx.Response(403, request=request)
    exc = httpx.HTTPStatusError("forbidden", request=request, response=response)
    result = should_retry(exc)
    assert result is False


def test_handle_error_lets_429_propagate():
    """handle_error does not convert 429 into ClientError — lets it propagate for retry."""
    from tiled.client.utils import handle_error

    request = httpx.Request("GET", "http://example.com/test")
    response = httpx.Response(429, headers={"Retry-After": "5"}, request=request)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        handle_error(response)
    # It should be a plain HTTPStatusError, not a ClientError
    from tiled.client.utils import ClientError

    assert not isinstance(exc_info.value, ClientError)
    assert exc_info.value.response.status_code == 429


def test_retry_context_retries_on_429():
    """retry_context retries when a 429 HTTPStatusError is raised."""
    stamina.set_active(True)
    try:
        attempts_made = 0
        for attempt in retry_context():
            with attempt:
                attempts_made += 1
                if attempts_made < 3:
                    request = httpx.Request("GET", "http://example.com/data")
                    response = httpx.Response(
                        429,
                        headers={"Retry-After": "0"},
                        request=request,
                    )
                    raise httpx.HTTPStatusError(
                        "Too Many Requests", request=request, response=response
                    )
        # Should have retried twice then succeeded on attempt 3.
        assert attempts_made == 3
    finally:
        stamina.set_active(False)


def test_retry_context_logs_429_retry(caplog):
    """429 retries are logged at DEBUG on tiled.client logger."""
    show_logs()
    stamina.set_active(True)
    try:
        attempts_made = 0
        with caplog.at_level(logging.DEBUG, logger="tiled.client"):
            for attempt in retry_context():
                with attempt:
                    attempts_made += 1
                    if attempts_made < 2:
                        request = httpx.Request("GET", "http://example.com/data")
                        response = httpx.Response(
                            429,
                            headers={"Retry-After": "0"},
                            request=request,
                        )
                        raise httpx.HTTPStatusError(
                            "Too Many Requests", request=request, response=response
                        )

        tiled_messages = [r for r in caplog.records if r.name == "tiled.client"]
        assert len(tiled_messages) >= 1
        assert "Retry" in tiled_messages[0].message
    finally:
        stamina.set_active(False)
        hide_logs()


def test_429_retry_with_real_server():
    """Integration test: client retries on 429 from a mock transport."""
    from tiled.client.utils import handle_error

    call_count = {"n": 0}

    class ThrottlingTransport(httpx.BaseTransport):
        def handle_request(self, request):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                return httpx.Response(
                    429,
                    headers={"Retry-After": "0"},
                )
            return httpx.Response(200, json={"status": "ok"})

    stamina.set_active(True)
    try:
        with httpx.Client(transport=ThrottlingTransport()) as client:
            for attempt in retry_context():
                with attempt:
                    response = client.get("http://testserver/data")
                    handle_error(response)

        assert call_count["n"] == 3
        assert response.json() == {"status": "ok"}
    finally:
        stamina.set_active(False)
