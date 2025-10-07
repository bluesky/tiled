from tiled.server.app import build_app
from tiled.catalog import from_uri
from tiled.client import Context, from_context
import logging
import sys
import h5py

from bluesky import RunEngine
from bluesky.callbacks.tiled_writer import TiledWriter
import logging
import bluesky.plans as bp
import numpy as np
from ophyd.sim import det
from ophyd.sim import hw
from pathlib import Path

from tiled.structures.core import StructureFamily, Spec
from tiled.structures.data_source import Asset, DataSource, Management

# Create and setup a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

# Initialize the catalog
catalog = from_uri(
    "postgresql://postgres:secret@localhost:5432/catalog",
    writable_storage={"filesystem": "file://localhost/tmp/tiled-catalog-data",
                      "sql": "postgresql://postgres:secret@localhost:5432/storage"}
)
logger.info(f"Initialized Tiled catalog {catalog}")

# Create some external HDF5 files to reference
hdf5_data_sources = []
for i in range(3):
    file_path = Path(f"/tmp/tiled-catalog-data/test_{i}.h5")
    with h5py.File(file_path, "w") as file:
        z = file.create_group("z")
        y = z.create_group("y")
        y.create_dataset("x", data=np.array([1, 2, 3]))
    asset = Asset(
            data_uri=f"file://localhost/{file_path}",
            is_directory=False,
            parameter="data_uris",
            num=0,
        )
    data_source = DataSource(
        mimetype="application/x-hdf5",
        assets=[asset],
        structure_family=StructureFamily.container,
        structure=None,
        parameters={"dataset": "z/y"},
        management=Management.external,
    )
    hdf5_data_sources.append(data_source)

# A simple example with a single 'streams' node to be deleted
with Context.from_app(build_app(catalog)) as context:
    client = from_context(context)
    primary = client.create_container("runs", specs=[Spec("CatalogOfBlueskyRuns", version='3.0')]) \
        .create_container("run_1", specs=[Spec("BlueskyRun", version='3.0')]) \
        .create_container("streams") \
        .create_container("primary", specs=[Spec("BlueskyEventStream", version='3.0'), Spec("composite")])
    primary.write_array(np.random.randn(3, 4), key="arr1")
    primary.write_array(np.random.randn(3, 4), key="arr2")


# Add more data
with Context.from_app(build_app(catalog)) as context:
    RE = RunEngine()
    client = from_context(context)
    runs_node = client["runs"]
    tw = TiledWriter(runs_node)
    RE.subscribe(tw)

    # 1. Some data from Bluesky (only these 6 'streams nodes should be deleted')
    for i in range(3):
        logger.info(f"Starting iteration {i}")
        ##### Internal Data Collection #####
        uid, = RE(bp.count([det], 3))
        
        #### External Data Collection #####
        Path("/tmp/tiled-catalog-data").mkdir(parents=True, exist_ok=True)
        uid, = RE(bp.count([hw(save_path="/tmp/tiled-catalog-data").img], 3))

    # 2. Add a stream node called "streams" -- should not be deleted
    stream_called_streams = runs_node[uid]["streams"].create_container("streams", specs=[Spec("BlueskyEventStream", version='3.0')])
    stream_called_streams.write_array(np.random.randn(3, 4), key="arr1")

    # 3. Create a BlueskyRun with an empty streams node -- should be deleted
    empty_run = runs_node.create_container("empty_run", specs=[Spec("BlueskyRun", version='3.0')])
    empty_run.create_container("streams")

    # 4. Create a BlueskyRun with no streams node containing an array -- should not be deleted
    non_empty_run = runs_node.create_container("non_empty_run", specs=[Spec("BlueskyRun", version='3.0')])
    non_empty_run.create_container("streams").write_array(np.random.randn(3, 4), key="arr1")

    # 5. Some other hierarchical data -- should not be deleted
    a = client.create_container("streams")
    b = a.create_container("b")
    c = b.create_container("streams")
    d = c.write_array([1, 2, 3], key="d")
    a.update_metadata({"color": "blue"})

    # 6. External HDF5 files
    a.new(
        structure_family=StructureFamily.container,
        data_sources=[hdf5_data_sources[0]],
        key="streams",
    )
    a.new(
        structure_family=StructureFamily.container,
        data_sources=[hdf5_data_sources[1]],
        key="hdf5_1",
    )
