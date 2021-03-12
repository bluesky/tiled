# Tiled

*Disclaimer: This is very early work, still in the process of defining scope.*

Tiled is a **data access** tool that enables search and structured, chunkwise
access to data in a variety of formats, regardless of the format the data
happens to be stored in. By providing the data to clients with semantic slicing
and a choice of formats, clients can access data with very light sofware
dependencies and fast partial downloads.

* Python-based *Catalogs* and *Readers* that integrate with directories of files
  or databases, such as MongoDB
* A Python-based HTTP server that makes data available to clients in **any
  language** in a variety of data formats
* A lightweight Python-based client whose only required dependency is
  [the httpx web client](https://www.python-httpx.org/)

**Lightweight core dependencies.** Both the server and client can operate with
few dependencies. (You don't even need numpy.) To serve and read numpy arrays,
dataframes, and xarray structures, install those respective libraries.
Additional optional dependencies expand the number of formats that the server
can provide (e.g. PNG, TIFF, Excel).

## Try it

Install, with the demo dependencies. (Note that this bundles in a bunch of the
optional dependencies.)

```
pip install tiled[demo]
```

Generate example data files.

```
python -m tiled.generate_example_data
```

Run server.

```
uvicorn tiled.server.main:api
```

Make requests. The server accepts JSON and msgpack. Once the server is running,
visit ``http://localhost:8000/docs`` for documentation. (Or, see below for
example requests and responses.)

The server serves a demo catalog by default, equivalent to:

```
ROOT_CATALOG="tiled.examples.generic:nested_with_access_control" uvicorn tiled.server.main:api
```

Other catalogs can be served by changing the value of the `ROOT_CATALOG`
environment variable to point to a different object in any importable Python
module.

Note: Directories are created in the current directory for scratch space. If
using uvicorn's ``--reload`` option, be sure to set
``--reload-dir=tiled`` to avoid reloading everytime a scratch file is
updated.
