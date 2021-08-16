from pathlib import Path
from ..client import from_config


def test_from_directory():
    """Tests that from_config with a Tree from a directory produces a node"""
    example_data_dir = Path(__file__).resolve().parent / "data"
    config = {
        "trees": [
            {
                "tree": "tiled.trees.files:Tree.from_directory",
                "path": "/",
                "args": {"directory": str(example_data_dir)},
            },
        ],
    }
    client = from_config(config)
    assert client["foo.csv"]
