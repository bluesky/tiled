import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
import yaml
from pydantic import ValidationError
from starlette.status import HTTP_400_BAD_REQUEST

from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context, from_profile, from_provider, record_history
from tiled.profiles import load_profiles, paths
from tiled.queries import Key
from tiled.server.app import build_app

from .utils import fail_with_status_code

tree = MapAdapter({})


def test_configurable_timeout():
    with Context.from_app(build_app(tree), timeout=httpx.Timeout(17)) as context:
        assert context.http_client.timeout.connect == 17
        assert context.http_client.timeout.read == 17


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


# ---------------------------------------------------------------------------
# from_provider() tests
# ---------------------------------------------------------------------------

# Patch targets – patch where the names are looked up (in constructors.py),
# not where they are defined (in context.py).
_CONTEXT = "tiled.client.constructors.Context"
_PASSWORD_GRANT = "tiled.client.constructors.password_grant"
_FROM_CONTEXT = "tiled.client.constructors.from_context"


def _make_provider_spec(name, mode="internal", auth_endpoint="/auth/provider/endpoint"):
    """Build a minimal provider spec object matching tiled's structure."""
    return SimpleNamespace(
        provider=name,
        mode=mode,
        links={"auth_endpoint": auth_endpoint},
    )


def _make_context(providers):
    """Build a mock Context with the given provider specs."""
    ctx = MagicMock()
    ctx.server_info.authentication.providers = providers
    return ctx


class TestFromProvider:
    """Tests for from_provider()."""

    def test_calls_context_from_any_uri(self):
        """Should call Context.from_any_uri with the given URI."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)
        mock_client = MagicMock()

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ) as mock_from_uri,
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=mock_client),
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            mock_from_uri.assert_called_once_with("http://localhost:8020")

    def test_resolves_correct_provider(self):
        """Should find the named provider and use its auth_endpoint."""
        providers = [
            _make_provider_spec("local", auth_endpoint="/auth/local"),
            _make_provider_spec("my_authenticator", auth_endpoint="/auth/aps-dm"),
        ]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}) as mock_grant,
            patch(_FROM_CONTEXT, return_value=MagicMock()),
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            mock_grant.assert_called_once_with(
                mock_context.http_client,
                "/auth/aps-dm",
                "my_authenticator",
                "user",
                "pass",
            )

    def test_calls_configure_auth_with_tokens(self):
        """Should call context.configure_auth() with the returned tokens."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)
        tokens = {"access_token": "abc", "refresh_token": "def"}

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value=tokens),
            patch(_FROM_CONTEXT, return_value=MagicMock()),
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            mock_context.configure_auth.assert_called_once_with(tokens)

    def test_sets_has_external_auth(self):
        """Should set context.has_external_auth = True after authentication."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=MagicMock()),
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            assert mock_context.has_external_auth is True

    def test_returns_from_context_result(self):
        """Should return the client produced by from_context()."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)
        mock_client = MagicMock(name="tiled_client")

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=mock_client) as mock_fc,
        ):
            result = from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            mock_fc.assert_called_once_with(
                mock_context,
                structure_clients="numpy",
                node_path_parts=[],
                include_data_sources=False,
            )
            assert result is mock_client

    def test_forwards_structure_clients(self):
        """Should forward structure_clients to from_context()."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=MagicMock()) as mock_fc,
        ):
            from_provider(
                "http://localhost:8020", "my_authenticator", "user", "pass", "dask"
            )

            assert mock_fc.call_args.kwargs["structure_clients"] == "dask"

    def test_forwards_node_path_parts(self):
        """Should forward node_path_parts from Context.from_any_uri to from_context()."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, ["a", "b", "c"]),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=MagicMock()) as mock_fc,
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            assert mock_fc.call_args.kwargs["node_path_parts"] == ["a", "b", "c"]

    def test_unknown_provider_raises_valueerror(self):
        """Should raise ValueError listing available providers."""
        providers = [
            _make_provider_spec("local"),
            _make_provider_spec("my_authenticator"),
        ]
        mock_context = _make_context(providers)

        with patch(
            _CONTEXT + ".from_any_uri",
            return_value=(mock_context, []),
        ):
            with pytest.raises(ValueError, match="no-such-provider") as exc_info:
                from_provider("http://localhost:8020", "no-such-provider", "user", "pass")

            # Error message should list available providers.
            msg = str(exc_info.value)
            assert "local" in msg
            assert "my_authenticator" in msg

    def test_no_providers_raises_valueerror(self):
        """Should raise ValueError when server has no providers."""
        mock_context = _make_context([])

        with patch(
            _CONTEXT + ".from_any_uri",
            return_value=(mock_context, []),
        ):
            with pytest.raises(ValueError, match="not found"):
                from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

    def test_external_provider_raises_valueerror(self):
        """Should raise ValueError for external (non-password) providers."""
        providers = [_make_provider_spec("oidc_provider", mode="external")]
        mock_context = _make_context(providers)

        with patch(
            _CONTEXT + ".from_any_uri",
            return_value=(mock_context, []),
        ):
            with pytest.raises(ValueError, match="does not support password-based"):
                from_provider("http://localhost:8020", "oidc_provider", "user", "pass")

    def test_password_mode_accepted(self):
        """Should accept providers with back-compat mode 'password'."""
        providers = [_make_provider_spec("legacy", mode="password")]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}),
            patch(_FROM_CONTEXT, return_value=MagicMock()),
        ):
            # Should not raise.
            from_provider("http://localhost:8020", "legacy", "user", "pass")

    def test_connection_error_propagates(self):
        """Connection errors from Context.from_any_uri should propagate."""
        with patch(
            _CONTEXT + ".from_any_uri",
            side_effect=ConnectionError("refused"),
        ):
            with pytest.raises(ConnectionError, match="refused"):
                from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

    def test_auth_error_propagates(self):
        """Authentication errors from password_grant should propagate."""
        providers = [_make_provider_spec("my_authenticator")]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(
                _PASSWORD_GRANT,
                side_effect=Exception("invalid credentials"),
            ),
        ):
            with pytest.raises(Exception, match="invalid credentials"):
                from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

    def test_first_matching_provider_is_used(self):
        """When multiple providers match, the first one should be used."""
        providers = [
            _make_provider_spec("my_authenticator", auth_endpoint="/auth/first"),
            _make_provider_spec("my_authenticator", auth_endpoint="/auth/second"),
        ]
        mock_context = _make_context(providers)

        with (
            patch(
                _CONTEXT + ".from_any_uri",
                return_value=(mock_context, []),
            ),
            patch(_PASSWORD_GRANT, return_value={"access_token": "tok"}) as mock_grant,
            patch(_FROM_CONTEXT, return_value=MagicMock()),
        ):
            from_provider("http://localhost:8020", "my_authenticator", "user", "pass")

            # Should use the first matching provider's endpoint.
            assert mock_grant.call_args[0][1] == "/auth/first"
