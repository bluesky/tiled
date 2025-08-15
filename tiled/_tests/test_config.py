from unittest.mock import Mock

from pydantic import ValidationError
import pytest
import yaml

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..config import Config, read_config
from ..server.app import build_app_from_config
from tiled import config

tree = MapAdapter({"example": ArrayAdapter.from_array([1, 2, 3])})


def test_root():
    "One tree served at top level"
    config = Config.model_validate({
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/",
            },
        ]
    })
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["example"]


def test_single_nested():
    "One tree served nested one layer down"
    config = Config.model_validate({
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a/b",
            },
        ]
    })
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b"]
        assert list(client["a"]["b"]) == ["example"]


def test_single_deeply_nested():
    "One tree served nested many layers down"
    config = Config.model_validate({
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a/b/c/d/e",
            },
        ]
    })
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client) == ["a"]
        assert list(client["a"]) == ["b"]
        assert list(client["a"]["b"]) == ["c"]
        assert list(client["a"]["b"]["c"]) == ["d"]
        assert list(client["a"]["b"]["c"]["d"]) == ["e"]
        assert list(client["a"]["b"]["c"]["d"]["e"]) == ["example"]


def test_many_nested():
    config = Config.model_validate({
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
    })
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        assert list(client["a"]["b"]) == ["example"]
        assert list(client["a"]["c"]) == ["example"]
        assert list(client["a"]["d"]["e"]) == ["example"]
        assert list(client["a"]["d"]["f"]) == ["example"]
        assert list(client["a"]["d"]["g"]["h"]) == ["example"]
        assert list(client["a"]["d"]["g"]["i"]) == ["example"]


# def test_extra_files(tmpdir):
#     config = {"trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}]}
#     with open(tmpdir / "config.yml", "w") as config_file:
#         yaml.dump(config, config_file)
#     with open(tmpdir / "README.md", "w") as extra_file:
#         extra_file.write("# Example")
#     read_config(str(tmpdir))


@pytest.mark.parametrize(
    "exists,error", [(True, "not a file or directory"), (False, "doesn't exist")]
)
def test_invalid_config_file_path(exists: bool, error: str):
    invalid = Mock()
    invalid.is_file.return_value = False
    invalid.is_dir.return_value = False
    invalid.exists.return_value = exists

    with pytest.raises(ValueError, match=error):
        read_config(invalid)


NEW_CONF = pytest.mark.pyd_conf


@NEW_CONF
@pytest.mark.parametrize(
    "path",
    [
        "example_configs/toy_authentication.yml",
        "example_configs/google_auth.yml",
        "example_configs/multiple_providers.yml",
        "example_configs/orcid_auth.yml",
        "example_configs/saml.yml",
        "example_configs/single_catalog_single_user.yml",
        "example_configs/small_single_user_demo.yml",
    ],
)
def test_example_configs(path):
    config.read_config(path)


@NEW_CONF
def test_pydantic_config():
    Config.model_validate({"trees": []})


@NEW_CONF
def test_duplicate_auth_providers():
    with pytest.raises(ValidationError, match="provider names must be unique"):
        Config.model_validate(
            {
                "authentication": {
                    "providers": [
                        {
                            "provider": "one",
                            "authenticator": "tiled.authenticators:DummyAuthenticator",
                        },
                        {
                            "provider": "one",
                            "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                        },
                    ]
                }
            }
        )


@NEW_CONF
@pytest.mark.parametrize(
    "paths", [("/", "/one"), ("/one/", "/one/two"), ("one/two", "one"), ("one", "one")]
)
def test_overlapping_trees(paths: tuple[str, ...]):
    with pytest.raises(ValidationError, match="cannot be subpaths of each other"):
        Config.model_validate(
            {"trees": [{"tree": "catalog", "path": path} for path in paths]}
        )


@NEW_CONF
def test_empty_api_key():
    with pytest.raises(ValidationError, match=r"should match pattern '\[a-zA-Z0-9\]\+'"):
        Config.model_validate({"authentication": {"single_user_api_key": ""}})
