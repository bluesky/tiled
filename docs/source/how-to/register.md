# Register Content in Tiled

The usage `tiled serve directory ...` is mostly for demos and small-scale use.
The following guide demonstrates more sophisticated control over this process.

## Quickstart

The tutorial {doc}`../tutorials/serving-files` demonstrates the usage:

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
about the files to specify particular `metadata` or `specs`.

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
                Asset(data_uri="file:///path/to/image1.tiff", is_directory=False, parameter="data_uri", num=1),
                Asset(data_uri="file:///path/to/image2.tiff", is_directory=False, parameter="data_uri", num=2),
            ],
        ),
    ],
    metadata={},
    specs=[],
)
```
