# Motivation and Goals

## Why structured data?

In the beginning there were raw bytes. Then numpy gave us strided N-dimensional
arrays. Now richer data structures with column structure, indexes/coordinates,
and additional context have grown.

Structure data enables us to navigate through a dataset remotely and download
only the parts we want. And once we have it, the structure makes the data easier
to understand and work with in data analysis.

## Prior Art

There are many solutions in this space. They frequently specialize on a data
Formapull t (e.g. [serving HDF5 files](https://github.com/jupyterlab/jupyterlab-hdf5)
or [Zarr](https://pypi.org/project/simple-zarr-server/)) or else on a kind of
dataset (e.g. large microscopy image stacks). This project specializes on the
popular scientific Python strucutres---N-dimensional array, DataFrame, and
xarray's various structures---reading tiles of data selectively on the server
side and providing them in a range of formats suitable to each given structure.

See also {doc}`lineage`.

## Requirements

* HTTP API that supports JSON and msgpack requests, with JSON and msgpack
  responses, as well as binary blob responses for chunked data
* Be usable from any HTTP client and from languages other that Python. Avoid
  baking any Python-isms deeply into it. No pickle, no msgpack-python. (Those
  can be *options*, for the purpose of communnicating with a Python client, but
  they should be priviledged or default.)
* Effeciently list and access entries, with pagation and random access.
* Efficiently search entries using an extensible text of queries including broad
  ones like "full text search" and more application- and dataset-specific
  queries.
* Access metadata cheaply.
* Serve data from numpy arrays, DataFrames, and xarray structures in various
  formats in a tile-based (chunked, partitioned) fashion.
* A Python client with rich proxy objects that do chunk-based access
  transparently (like Intake's `RemoteXarray` and similar). But, differently
  from current Intake and Databroker, do not switch dask-vs-not-dask or
  dask-vs-another-delayed-framework at call time. Use a consistent delayed
  framework (or none at all) consistently within a given context. Your only
  option at call time should be `read()`. Whether that is in memory, dask, or
  something else should be set higher up---for example, on the client instance.
* Usable performance without any *intrinsic* caching in the server. Objects may
  do some internal caching for optimization, but the server will not
  explicitly hang on to any state between requests.
* Path toward adding state / caching in external systems (e.g. Redis, nginx)


