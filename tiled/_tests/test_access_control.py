import io
import json

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_config

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))


def tree_a(access_policy=None):
    return MapAdapter({"A1": arr, "A2": arr}, access_policy=access_policy)


def tree_b(access_policy=None):
    return MapAdapter({"B1": arr, "B2": arr}, access_policy=access_policy)


@pytest.fixture
def config(tmpdir):
    return {
        "authentication": {
            "secret_keys": ["SECRET"],
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {
                        "users_to_passwords": {"alice": "secret1", "bob": "secret2"}
                    },
                }
            ],
        },
        "database": {
            "uri": f"sqlite:///{tmpdir}/tiled.sqlite",
        },
        "access_control": {
            "access_policy": "tiled.access_policies:SimpleAccessPolicy",
            "args": {"access_lists": {"alice": ["a"]}, "provider": "toy"},
        },
        "trees": [
            {
                "tree": f"{__name__}:tree_a",
                "path": "/a",
                "access_control": {
                    "access_policy": "tiled.access_policies:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "access_lists": {
                            "alice": ["A2"],
                            # This should have no effect because bob
                            # cannot access the parent node.
                            "bob": ["A1", "A2"],
                        },
                    },
                },
            },
            {"tree": f"{__name__}:tree_b", "path": "/b", "access_policy": None},
        ],
    }


def test_top_level_access_control(enter_password, config):
    with enter_password("secret1"):
        alice_client = from_config(config, username="alice", token_cache={})
    with enter_password("secret2"):
        bob_client = from_config(config, username="bob", token_cache={})
    assert "a" in alice_client
    assert "A2" in alice_client["a"]
    assert "A1" not in alice_client["a"]
    assert "b" not in alice_client
    alice_client["a"]["A2"]
    with pytest.raises(KeyError):
        alice_client["b"]
    assert not list(bob_client)
    with pytest.raises(KeyError):
        bob_client["a"]
    with pytest.raises(KeyError):
        bob_client["b"]


def test_node_export(enter_password, config):
    "Exporting a node should include only the children we can see."
    with enter_password("secret1"):
        alice_client = from_config(config, username="alice", token_cache={})
    buffer = io.BytesIO()
    alice_client.export(buffer, format="application/json")
    buffer.seek(0)
    exported_dict = json.loads(buffer.read())
    assert "a" in exported_dict["contents"]
    assert "A2" in exported_dict["contents"]["a"]["contents"]
    assert "A1" not in exported_dict["contents"]["a"]["contents"]
    assert "b" not in exported_dict
    exported_dict["contents"]["a"]["contents"]["A2"]
