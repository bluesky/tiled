import io
import os

import h5py
import httpx
from tiled.catalogs.hdf5 import Catalog


# Download a Nexus file into a memory buffer.
DEFAULT_EXAMPLE_URL = "https://github.com/nexusformat/exampledata/blob/master/APS/EPICSareaDetector/hdf5/AgBehenate_228.hdf5?raw=true"  # noqa
buffer = io.BytesIO(httpx.get(os.getenv("NEXUS_URL", DEFAULT_EXAMPLE_URL)).content)
# Access the buffer with h5py, which can treat it like a "file".
file = h5py.File(buffer, "r")
# Wrap the h5py.File in a Catalog to serve it with Tiled.
catalog = Catalog(file)
