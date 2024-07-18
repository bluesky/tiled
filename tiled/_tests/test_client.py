import logging
from pathlib import Path

import httpx
import pytest
import yaml
from starlette.status import HTTP_400_BAD_REQUEST

from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, from_profile, record_history
from ..config import ConfigError
from ..profiles import load_profiles, paths
from ..queries import Key
from ..server.app import build_app
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
        with pytest.raises(ConfigError):
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
