# Register Content in Tiled

The usage `tiled serve directory ...` is mostly for demos and small-scale use.
The following guide demonstrates more sophisticated control over this process.

## Quickstart

You can use

```
tiled serve directory [--watch] [--public] [--api-key <SECRET>] <DIRECTORY>
```

which is a shorthand for:

1. Walk a directory tree to identify formats it recognizes and then ingest their
   metadata, structure, and filepaths into a database for efficient search and
   random access.
2. Start a server that uses that data.
3. Optionally, watch the directory tree for changes, and synchronizing them to
   the data.

### Limitations of `tiled serve directory ...`

The shorthand is great for quickly getting started, but it has numerous
limitations.

- Tiled walks the entire directory at server startup. This can be slow.
- Tiled creates an ephemeral database (SQLite in a temporary directory)
  just for this process. That work is discarded when the server shuts down.
- One database per server is not horizontally scalable.
- With `--watch`, Tiled picks up files as soon as they are created, and
  they may not be ready to be read yet. (Example: a partially-written HDF5
  file.)
- With `--watch`, Tiled currently re-scans the entire directory from scratch
  every time anything changes. This may be improved in the future, but there
  are limitations to how smooth this can be.
- This can place a lot of load on a filesystem, which can be an issue for
  networked file systems in particular.

When these limitations are reached, read on for a more sophisticated approach.

## Production-Scale Approach

Start a Tiled server.

```
tiled serve catalog <DATABASE_URI> -r <DIRECTORY> [--public] [--api-key <SECRET>]
```

- The `<DATABASE_URI>` may be a SQLite file like `catalog.db` or a PostgreSQL
  URI like `postgresql://<USERNAME>:<PASSWORD>@<HOST>/<DATABASE>`.
- The `<DIRECTORY>` instructs Tiled to enable an authorized clients to register
  files in that directory to be served. For security reasons, nothing outside
  of that directory will be possible to register. (Multiple `-r` arguments may
  be used.)
- If an `--api-key` is not passed, a secure random key will be generated and
  printed at server startup.

### Simple cases

As in the Quickstart, this walks the directory tree, identifies recognized
formats, and registers the metadata, structure, and filepaths.

```
tiled register http://localhost:8000 [--api-key <SECRET>] <DIRECTORY>
```

### Complex cases

Sometimes it is necessary to take more manual control of this registration
process, such as if you want to take advantage of particular knowledge
about the files to specify particular `metadata` or `specs`, or to register
just a specific file (or files) rather than walking a whole directory.

For this, use the Python client method `Container.register`. It registers
*exactly* the file(s) you give it as a single dataset. Unlike
`tiled serve directory ...` or `tiled register ...`, it does **not** walk
directories and does not create a container of sub-nodes. As in the previous
example, the files stay where they are, in their original format; Tiled
just records how to read them in its catalog.

```py
from tiled.client import from_uri

# For security, prefer setting the API key in the environment variable
# TILED_API_KEY, which from_uri(...) detects automatically.
client = from_uri("http://localhost:8000", api_key="...")
```

The files must be reachable from **both** the client (to inspect them and
infer their structure) and the server (to serve the data), and they must live
under one of the server's registered readable directories (the `-r <DIRECTORY>`
above).

#### Register a single file

The mimetype -- and thus the structure family (array, table, ...) -- is inferred
from the file extension.

```py
# A single image file becomes an 'array' node.
client.register("/data/beamline/scan_001.tiff")

# A CSV file becomes a 'table' node.
client.register("/data/beamline/measurements.csv")
```

By default the server assigns a random key (the node name), just like
`write_array`, `write_table`, etc. Pass `key=...` to define it yourself:

```py
client.register("/data/beamline/scan_001.tiff", key="scan_001")
```

#### Register several files as one stacked array

Multiple URIs can be combined into a *single* dataset assembled from homogeneous
files -- for example, a stack of TIFF images forming one 3D array. This creates
a single node in Tiled, not one node per file.

```py
client.register(
    "/data/beamline/frame_000.tiff",
    "/data/beamline/frame_001.tiff",
    "/data/beamline/frame_002.tiff",
    key="scan_stack",
)
```

#### Register a directory-based format

A single URI pointing at a directory (for example, a Zarr store) is registered
as one dataset backed by that directory. (The directory is treated as a unit;
it is not walked.)

```py
client.register("/data/beamline/image.zarr", key="image")
```

#### Attach metadata and specs

Some formats carry their own metadata, which Tiled reads from the file(s). The
`metadata` you supply is merged with it, taking precedence on any conflicting
keys. Any `specs` you supply are attached to the node.

```py
from tiled.structures.core import Spec

client.register(
    "/data/beamline/scan_001.tiff",
    key="scan_001",
    metadata={"beamline": "X", "sample": "Fe2O3"},
    specs=[Spec("xrd_scan")],
)
```

#### Pass options for opening the file(s)

Some formats need extra options to be read correctly -- for example, the
delimiter of a CSV or a path within a complex hierarchical HDF5 file. Pass these
as a `parameters` dictionary. Tiled applies them both when it inspects the file(s)
on the client and when the server later re-opens them to serve the data.

```py
# A semicolon-delimited CSV.
client.register(
    "/data/beamline/european.csv",
    parameters={"sep": ";"},
)
```

For HDF5, `parameters={"dataset": ...}` selects a specific dataset (which may
be nested in a group) and registers it as an `array` node, instead of
registering the whole file as a `container` of its datasets:

```py
# Register the whole HDF5 file as a container exposing its datasets.
client.register("/data/beamline/run.h5", key="run")

# Register just one dataset within it as an array.
client.register(
    "/data/beamline/run.h5",
    key="intensity",
    parameters={"dataset": "entry/data/intensity"},
)
```

#### Override the mimetype

By default the mimetype is inferred from the file extension. Override it to
interpret a file differently. For example, a CSV is normally read as a table;
the mimetype below reads it as an array instead, with `skiprows=1` dropping the
header row so only the numeric values remain:

```py
client.register(
    "/data/beamline/values.csv",
    key="values",
    mimetype="text/csv;header=absent",
    parameters={"skiprows": 1},
)
```

When reading a CSV as an array, its columns should share a single dtype (for
example, all floats), since an array has one homogeneous dtype.

#### Override the inferred structure

Tiled infers the structure from the file(s), but you can supply your own (compatible)
`structure` to change how the data is presented -- for example, to reshape a flat
array when it is served. The structure you provide must still be able to
describe the underlying data: arrays must match in dtype and total number of
elements, and tables in column count and dtypes.

`structure` may be given as a plain dict (the JSON form), so no imports are
needed:

```py
# The file holds 12 uint16 elements; serve them as a (2, 6) array.
client.register(
    "/data/beamline/scan_001.tiff",
    key="reshaped",
    structure={
        "data_type": {"endianness": "little", "kind": "u", "itemsize": 2},
        "shape": [2, 6],
        "chunks": [[2], [6]],
        "dims": None,
    },
)
```

### Low-level registration

`Container.register` is a convenience wrapper over the low-level `new` method,
which gives you complete manual control over the `DataSource` and `Asset`
description. Reach for this only when `register` does not expose what you need.

Use the Python client, as in this example.

```py
import numpy
from tiled.client import from_uri
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.structures.array import ArrayStructure, BuiltinDtype

# You can pass the api_key in explicitly as shown here, but for security, it
# is best to set the API key in the environment variable TILED_API_KEY, which
# from_uri(...) will automatically detect and use.
client = from_uri("http://localhost:8000", api_key="...")

structure = ArrayStructure(
    data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype('int32')),
    shape=(2, 512, 512),
    chunks=((1, 1), (512,), (512,)),
    dims=("time", "x", "y"),  # optional
)

# POST /api/v1/register/{path}
client.new(
    structure_family=StructureFamily.array,
    data_sources=[
        DataSource(
            management=Management.external,
            mimetype="multipart/related;type=image/tiff",
            structure_family=StructureFamily.array,
	    structure=structure,
            assets=[
                Asset(data_uri="file:///path/to/image1.tiff", is_directory=False, parameter="data_uris", num=0),
                Asset(data_uri="file:///path/to/image2.tiff", is_directory=False, parameter="data_uris", num=1),
            ],
        ),
    ],
    metadata={},
    specs=[],
)
```
