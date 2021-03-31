# Tiled

*Disclaimer: This is very early work, still in the process of defining scope.*

Data analysis is easier and better when we load and operate on data in simple,
self-describing structures that keep our mind on the science rather the
book-keeping of filenames and file formats.

Tiled is a **data access** tool that enables **search** and **structured,
chunkwise access** to data in a **variety of formats**, regardless of the
format the data happens to be stored in at rest. Like Jupyter, Tiled can be
used solo or deployed as a shared resource. Tiled provides data, locally or
over the web, in a choice of formats, spanning slow but widespread
interchange formats (e.g. CSV, JSON, TIFF) and fast, efficient ones (C
buffers, Apache Arrow DataFrames). Tiled enables slicing and sub-selection
for accessing only the data of interest, and it enables parallelized download
of many chunks at once. Users can access data with very light software
dependencies and fast partial downloads.

Web-based data access usually involves downloading complete files, in the
manner of [Globus](https://www.globus.org/); or using modern chunk-based
storage formats, such as [TileDB](https://tiledb.com/) and
[Zarr](https://zarr.readthedocs.io/en/stable/) in local or cloud storage; or
using custom solutions tailored to a particular large dataset. Waiting for an
entire file to download when only the first frame of an image stack or a
certain column of a table are of interest is wasteful and can be prohibitive
for large longitudinal analyses. Yet, it is not always practical to transcode
the data into a chunk-friendly format or build a custom tile-based-access
solution. (Though if you can do either of those things, you should consider
them instead!)

In more technical language, Tiled is an HTTP service providing "tiled"
chunk-based access to strided arrays, tablular datasets ("dataframes"), and
nested structures thereof. It is centered on these *structures* (backed by
numpy, pandas, xarray, various mappings in the server) rather than particular
formats. The structures may be read from an extensible range of formats; the
web client receives them as one of an extensible range of MIME types, and it
can pose requests that sub-select and slice the data before it is read or
served. The server incorporates search capability, which may be backed by a
proper database solution at large scales or a simple in-memory index at small
scales, as well as access controls. A Python client provides a friendly
h5py-like interface and supports offline caching.

**Docs are not yet published. See ``docs/source/`` directory in source tree.**