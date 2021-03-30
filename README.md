# Tiled

*Disclaimer: This is very early work, still in the process of defining scope.*

Tiled is a **data access** tool that enables search and structured, chunkwise
access to data in a variety of formats, regardless of the format the data
happens to be stored in. The server provides data to clients with semantic
slicing and a choice of formats, so users can access data with very light
software dependencies and fast partial downloads.

Web-based data access usually involves downloading complete files, in the
manner of [Globus](https://www.globus.org/); or using specialized chunk-based
storage formats, such as [TileDB](https://tiledb.com/) and
[Zarr](https://zarr.readthedocs.io/en/stable/) in local or cloud storage; or
using custom solutions tailored to a particular large dataset. Waiting for an
entire file to download when only the first frame of an image stack or a
certain column of a table are of interest is wasteful and can be prohibitive
for large longitudinal analyses. Yet, it is not always practical to transcode
the data into a chunk-friendly format or build a custom tile-based-access
solution.

Tiled is an HTTP service providing "tiled" chunk-based access to strided
arrays, tablular datasets ("dataframes"), and nested structures thereof. It
is centered on these *structures* (backed by numpy, pandas, xarray, various
mappings in the server) rather than particular formats. The structures may be
read from an extensible range of formats; the web client receives them as one
of an extensible range of MIME types, and it can pose requests that
sub-select and slice the data before it is read or served. The server
incorporates search capability, which may be backed by a proper database
solution at large scales or a simple in-memory index at small scales, as well
as access controls. A Python client provides a friendly h5py-like interface
and supports offline caching.

## Try it

Install the dependencies for the client, the server, xarray, and h5py to
generate example data files.

```
pip install tiled[client,server,xarray] h5py
```

Generate example data files.

```
python -m tiled.generate_example_data
```

Run server with a demo catalog.

```
tiled serve pyobject tiled.examples.generic:demo
```

Visit ``http://localhost:8000/docs`` for interactive documentation.