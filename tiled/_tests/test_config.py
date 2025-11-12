from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi import APIRouter
from pydantic import ValidationError

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..config import Config, parse_configs
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
    import yaml

    config = {"trees": [{"path": "/", "tree": "tiled.examples.generated_minimal:tree"}]}
    with open(tmpdir / "config.yml", "w") as config_file:
        yaml.dump(config, config_file)
    with open(tmpdir / "README.md", "w") as extra_file:
        extra_file.write("# Example")
    parse_configs(str(tmpdir))


def test_multi_file_trees(tmpdir):
    "Test that 'trees' can be specified across more than one file, merged."
    import yaml

    conf1 = {"trees": [{"path": "/a", "tree": "tiled.examples.generated_minimal:tree"}]}
    conf2 = {"trees": [{"path": "/b", "tree": "tiled.examples.generated_minimal:tree"}]}
    with open(tmpdir / "conf1.yml", "w") as c1:
        yaml.dump(conf1, c1)
    with open(tmpdir / "conf2.yml", "w") as c2:
        yaml.dump(conf2, c2)
    config = parse_configs(tmpdir)
    assert len(config.trees) == 2


def test_multi_file_conflict(tmpdir):
    "Test that media_types can only be specified in a single config file."
    import yaml

    conf1 = {
        "media_types": {},
        "trees": [{"path": "/a", "tree": "tiled.examples.generated_minimal:tree"}],
    }
    conf2 = {
        "media_types": {},
        "trees": [{"path": "/b", "tree": "tiled.examples.generated_minimal:tree"}],
    }
    with open(tmpdir / "conf1.yml", "w") as c1:
        yaml.dump(conf1, c1)
    with open(tmpdir / "conf2.yml", "w") as c2:
        yaml.dump(conf2, c2)
    with pytest.raises(ValueError, match="Duplicate configuration for {'media_types'}"):
        parse_configs(tmpdir)


@pytest.mark.parametrize(
    "path,error",
    [
        ("", ValidationError),  # Assumes no config files in root of tmpdir -> no trees
        ("non-existent-file", FileNotFoundError),
    ],
)
def test_invalid_config_file_path(tmpdir: Path, path: str, error: type[Exception]):
    with pytest.raises(error):
        parse_configs(tmpdir / path)


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
    parse_configs(path)


def test_pydantic_config():
    Config.model_validate({"trees": []})


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


@respx.mock
def test_proxied_authenticator_single_instance_required(
    well_known_response: dict[str, Any]
):
    respx.get("http://example.com").mock(
        side_effect=[
            httpx.Response(httpx.codes.OK, json=well_known_response),
            httpx.Response(httpx.codes.OK, json=well_known_response),
        ]
    )
    with pytest.raises(
        ValidationError,
        match="Multiple ProxiedOIDCAuthenticator instances are configured.",
    ):
        Config.model_validate(
            {
                "trees": [],
                "authentication": {
                    "providers": [
                        {
                            "provider": "one",
                            "authenticator": "tiled.authenticators:ProxiedOIDCAuthenticator",
                            "args": {
                                "audience": "tiled",
                                "client_id": "tiled",
                                "device_flow_client_id": "tiled-cli",
                                "well_known_uri": "http://example.com",
                            },
                        },
                        {
                            "provider": "two",
                            "authenticator": "tiled.authenticators:ProxiedOIDCAuthenticator",
                            "args": {
                                "audience": "tiled",
                                "client_id": "tiled",
                                "device_flow_client_id": "tiled-cli",
                                "well_known_uri": "http://example.com",
                            },
                        },
                    ]
                },
            }
        )


@respx.mock
def test_proxied_authenticator_is_not_used_with_other_authenticators(
    well_known_response: dict[str, Any],
):
    respx.get("http://example.com").mock(
        return_value=httpx.Response(httpx.codes.OK, json=well_known_response)
    )
    with pytest.raises(
        ValidationError,
        match="ProxiedOIDCAuthenticator must not be configured together with other authentication providers.",
    ):
        Config.model_validate(
            {
                "trees": [],
                "authentication": {
                    "providers": [
                        {
                            "provider": "one",
                            "authenticator": "tiled.authenticators:DummyAuthenticator",
                        },
                        {
                            "provider": "two",
                            "authenticator": "tiled.authenticators:ProxiedOIDCAuthenticator",
                            "args": {
                                "audience": "tiled",
                                "client_id": "tiled",
                                "well_known_uri": "http://example.com",
                                "device_flow_client_id": "tiled-cli",
                            },
                        },
                    ]
                },
            }
        )


@pytest.mark.parametrize(
    "paths", [("/", "/one"), ("/one/", "/one/two"), ("one/two", "one"), ("one", "one")]
)
def test_overlapping_trees(paths: tuple[str, ...]):
    with pytest.raises(ValidationError, match="cannot be subpaths of each other"):
        Config.model_validate(
            {"trees": [{"tree": "catalog", "path": path} for path in paths]}
        )


def test_empty_api_key():
    with pytest.raises(
        ValidationError, match=r"should match pattern '\[a-zA-Z0-9\]\+'"
    ):
        Config.model_validate({"authentication": {"single_user_api_key": ""}})


class Dummy:
    "Referenced below in test_tree_given_as_method"

    def constructor():
        return tree


def test_tree_given_as_method():
    config = {
        "trees": [
            {
                "tree": f"{__name__}:Dummy.constructor",
                "path": "/",
            },
        ]
    }
    Config.model_validate(config)


tree.include_routers = [APIRouter()]


def test_include_routers():
    config = {
        "trees": [
            {
                "tree": f"{__name__}:tree",
                "path": "/a",
            },
            {
                "tree": f"{__name__}:tree",
                "path": "/b",
            },
        ]
    }
    app = build_app_from_config(config)
    with Context.from_app(app) as context:
        from_context(context)
