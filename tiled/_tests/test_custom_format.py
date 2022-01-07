import io

from ..adapters.mapping import MapAdapter
from ..client import from_config
from ..examples.xdi import XDIDataFrameAdapter, data, read_xdi

tree = MapAdapter({"example": XDIDataFrameAdapter.from_file(io.StringIO(data))})


def test_xdi_round_trip():
    """
    Steps:

    1. Parse XDI string into DataFrame + dict.
    2. Export as XDI to client.
    3. Read exported XDI into DataFrame + dict.

    Compare result of (3) to result of (1).
    """
    config = {
        "trees": [{"tree": "tiled._tests.test_custom_format:tree", "path": "/"}],
        "media_types": {
            "dataframe": {"application/x-xdi": "tiled.examples.xdi:write_xdi"}
        },
        "file_extensions": {"xdi": "application/x-xdi"},
    }
    client = from_config(config)
    buffer = io.BytesIO()
    client["example"].export(buffer, format="xdi")
    # Let read_xdi view this as a text buffer, rewound to 0.
    buffer.seek(0)
    str_buffer = io.TextIOWrapper(buffer, encoding="utf-8")
    actual_df, actual_md = read_xdi(str_buffer)
    # Remove the "comments" before making a comparison
    # because we add a comment line during serialization.
    actual_md.pop("comments")
    expected_md = dict(client["example"].metadata)
    expected_md.pop("comments")
    assert actual_md == expected_md
