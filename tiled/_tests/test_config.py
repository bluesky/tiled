from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_config

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
    client = from_config(config)
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
    client = from_config(config)
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
    client = from_config(config)
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
    client = from_config(config)
    assert list(client["a"]["b"]) == ["example"]
    assert list(client["a"]["c"]) == ["example"]
    assert list(client["a"]["d"]["e"]) == ["example"]
    assert list(client["a"]["d"]["f"]) == ["example"]
    assert list(client["a"]["d"]["g"]["h"]) == ["example"]
    assert list(client["a"]["d"]["g"]["i"]) == ["example"]
