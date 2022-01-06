"""
Use this examples like:

tiled serve pyobject --public tiled.examples.nexus:tree

To serve a different URL from the example hard-coded here, use the config:

```
# config.yml
authentication:
    allow_anonymous_access: true
trees:
    - path: /
      tree: tiled.examples.nexus:MapAdapter
      args:
          url: YOUR_URL_HERE
```

tiled serve config config.yml
"""
import io

import h5py
import httpx

from tiled.adapters.hdf5 import HDF5Adapter


def build_tree(url):
    # Download a Nexus file into a memory buffer.
    buffer = io.BytesIO(httpx.get(url).content)
    # Access the buffer with h5py, which can treat it like a "file".
    file = h5py.File(buffer, "r")
    # Wrap the h5py.File in a MapAdapter to serve it with Tiled.
    return HDF5Adapter(file)


EXAMPLE_URL = "https://github.com/nexusformat/exampledata/blob/master/APS/EPICSareaDetector/hdf5/AgBehenate_228.hdf5?raw=true"  # noqa
tree = build_tree(EXAMPLE_URL)
