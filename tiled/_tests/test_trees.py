import io

from ..client import from_config
from ..trees.in_memory import Tree


def test_from_directory():
    """
    Steps:

    1. Parse XDI string into DataFrame + dict.
    2. Export as XDI to client.
    3. Read exported XDI into DataFrame + dict.

    Compare result of (3) to result of (1).
    """
    config = {
        "trees": [
            {
                "tree": "tiled.trees.files:Tree.from_directory",
                "path": "/data",
            },
        ],

    }
    client = from_config(config)
    print(client)
    buffer = io.BytesIO()
    print(client["data"])
    # client["data"].export(buffer)
    # # Let read_xdi view this as a text buffer, rewound to 0.
    # buffer.seek(0)
    # str_buffer = io.TextIOWrapper(buffer, encoding="utf-8")

    # actual_df, actual_md = read_xdi(str_buffer)
    # # Remove the "comments" before making a comparison
    # # because we add a comment line during serialization.
    # actual_md.pop("comments")
    # expected_md = dict(client["example"].metadata)
    # expected_md.pop("comments")
    # assert actual_md == expected_md