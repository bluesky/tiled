from datetime import datetime

import numpy
import pytest

from ..readers.array import ArrayAdapter
from ..trees.in_memory import Tree
from ..client import from_config
from ..server.authentication import create_refresh_token

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))


def tree_a(access_policy):
    return Tree({"A1": arr, "A2": arr}, access_policy=access_policy)


def tree_b(access_policy):
    return Tree({"B1": arr, "B2": arr}, access_policy=access_policy)


def test_top_level_access_control():
    SECRET_KEY = "secret"
    config = {
        "authentication": {
            "secret_keys": [SECRET_KEY],
            "authenticator": "tiled.authenticators:DictionaryAuthenticator",
            "args": {"users_to_passwords": {"alice": "secret1", "bob": "secret2"}},
        },
        "access_control": {
            "access_policy": "tiled.trees.in_memory:SimpleAccessPolicy",
            "args": {"access_lists": {"alice": ["a"]}},
        },
        "trees": [
            {
                "tree": f"{__name__}:tree_a",
                "path": "/a",
                "access_control": {
                    "access_policy": "tiled.trees.in_memory:SimpleAccessPolicy",
                    "args": {
                        "access_lists": {
                            "alice": ["A2"],
                            # This should have no effect because bob
                            # cannot access the parent node.
                            "bob": ["A1", "A2"],
                        }
                    },
                },
            },
            {
                "tree": f"{__name__}:tree_b",
                "path": "/b",
            },
        ],
    }
    # Directly generate a refresh token.
    alice_refresh_token = create_refresh_token(
        data={"sub": "alice"},
        session_id=0,
        session_creation_time=datetime.now(),
        secret_key=SECRET_KEY,
    )
    bob_refresh_token = create_refresh_token(
        data={"sub": "bob"},
        session_id=0,
        session_creation_time=datetime.now(),
        secret_key=SECRET_KEY,
    )
    # Provide the refresh token in a token cache. The client
    # will use this to "refresh" and obtain an access token.
    alice_client = from_config(
        config, username="alice", token_cache={"refresh_token": alice_refresh_token}
    )
    bob_client = from_config(
        config, username="bob", token_cache={"refresh_token": bob_refresh_token}
    )
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
