from datetime import datetime

import numpy

from ..readers.array import ArrayAdapter
from ..trees.in_memory import Tree
from ..client import from_config
from ..server.authentication import create_refresh_token

arr = ArrayAdapter.from_array(numpy.ones((5, 5)))
tree_a = Tree({"A1": arr, "A2": arr})
tree_b = Tree({"B1": arr, "B2": arr})


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
            },
            {
                "tree": f"{__name__}:tree_b",
                "path": "/b",
            },
        ],
    }
    # Directly generate a refresh token.
    refresh_token = create_refresh_token(
        data={"sub": "alice"},
        session_id=0,
        session_creation_time=datetime.now(),
        secret_key=SECRET_KEY,
    )
    # Provide the refresh token in a token cache. The client
    # will use this to "refresh" and obtain an access token.
    client = from_config(
        config, username="alice", token_cache={"refresh_token": refresh_token}
    )
    assert "a" in client
    # Try accessing an item nested inside an item we can access.
    client["a"]["A1"]
    assert "b" not in client
