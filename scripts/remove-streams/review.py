import logging
import sys

import bluesky.plans as bp
from bluesky import RunEngine
from bluesky.callbacks.tiled_writer import TiledWriter
from ophyd.sim import hw

from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app

# Create and setup a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

catalog_dst = from_uri(
    "postgresql://postgres:secret@localhost:5432/catalog",
    writable_storage={
        "filesystem": "file://localhost/tmp/tiled-catalog-data",
        "sql": "postgresql://postgres:secret@localhost:5432/storage",
    },
)
app = build_app(catalog_dst)


def recursve_read(client):
    for name, child in client.items():
        logger.info(f"Reading node: {name}")
        if child.structure_family == "container":
            recursve_read(child)
        else:
            result = child.read()
            logger.info(f">            {result}")


with Context.from_app(app) as context:
    client = from_context(context)
    recursve_read(client)

    # Write some data
    RE = RunEngine()
    tw = TiledWriter(client["runs"])
    RE.subscribe(tw)
    (uid,) = RE(bp.count([hw(save_path="/tmp/tiled-catalog-data").img], 3))

context = Context.from_app(app)
client = from_context(context)
