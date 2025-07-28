from unittest.mock import Mock

import pytest
import yaml

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..config import parse_configs
from ..server.app import build_app_from_config

tree = MapAdapter({"example": ArrayAdapter.from_array([1, 2, 3])})


def test_root():
    "One tree served at top level"
    config = {
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ]
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["example"]


def test_single_nested():
    "One tree served nested one layer down"
    config = {
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a/b",
            },
        ]
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b"]
        assert list(client["a"]["b"]) == ["example"]


def test_single_deeply_nested():
    "One tree served nested many layers down"
    config = {
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a/b/c/d/e",
            },
        ]
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b"]
        assert list(client["a"]["b"]) == ["c"]
        assert list(client["a"]["b"]["c"]) == ["d"]
        assert list(client["a"]["b"]["c"]["d"]) == ["e"]
        assert list(client["a"]["b"]["c"]["d"]["e"]) == ["example"]


def test_many_nested():
    config = {
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a/b",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/a/c",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/a/d/e",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/a/d/f",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/a/d/g/h",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/a/d/g/i",
            },
        ],
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client["a"]["b"]) == ["example"]
        assert list(client["a"]["c"]) == ["example"]
        assert list(client["a"]["d"]["e"]) == ["example"]
        assert list(client["a"]["d"]["f"]) == ["example"]
        assert list(client["a"]["d"]["g"]["h"]) == ["example"]
        assert list(client["a"]["d"]["g"]["i"]) == ["example"]


def test_extra_files(tmpdir):
    config = {"trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}]}
    with open(tmpdir / "config.yml", "w") as config_file:
        yaml.dump(config, config_file)
    with open(tmpdir / "README.md", "w") as extra_file:
        extra_file.write("# Example")
    parse_configs(str(tmpdir))


@pytest.mark.parametrize(
    "exists,error", [(True, "not a file or directory"), (False, "doesn't exist")]
)
def test_invalid_config_file_path(exists: bool, error: str):
    invalid = Mock()
    invalid.is_file.return_value = False
    invalid.is_dir.return_value = False
    invalid.exists.return_value = exists

    with pytest.raises(ValueError, match=error):
        parse_configs(invalid)
