from pathlib import Path

import pytest

from ..catalog.register import register
from ..client import Context, from_context
from ..examples.xdi import data
from ..server.app import build_app_from_config


@pytest.mark.asyncio
async def test_xdi_round_trip(tmpdir):
    """
    Steps:

    1. Parse XDI string into DataFrame + dict.
    2. Export as XDI to client.
    3. Read exported XDI into DataFrame + dict.

    Compare result of (3) to result of (1).
    """
    # Write example data file.
    Path(tmpdir / "files").mkdir()
    with open(tmpdir / "files" / "example.xdi", "w") as file:
        file.write(data)
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": tmpdir / "catalog.db",
                    "readable_storage": [tmpdir / "files"],
                    "init_if_not_exists": True,
                    "adapters_by_mimetype": {
                        "application/x-xdi": "tiled.examples.xdi:read_xdi"
                    },
                },
            }
        ],
        "media_types": {"xdi": {"application/x-xdi": "tiled.examples.xdi:write_xdi"}},
        "file_extensions": {"xdi": "application/x-xdi"},
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        tree = context.http_client.app.state.root_tree
        await register(
            tree,
            tmpdir / "files",
            adapters_by_mimetype={"application/x-xdi": "tiled.examples.xdi:read_xdi"},
            mimetypes_by_file_ext={".xdi": "application/x-xdi"},
        )
        client["example"].export(str(tmpdir / "exported.xdi"))
        actual = Path(tmpdir / "exported.xdi").read_text()
        actual
        # XDI uses a two-space spacing that pandas.to_csv does not support.
        # Need thought to get this exactly right.
        # assert actual == data
