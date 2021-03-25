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

Visit ``http://localhost:8000/docs`` for interactive documentation. Or, see
below for example requests and responses.

This can work with various front-ends. Try it for example with Plotly's Chart
Studio or Vega Voyager.

Start the server in a way that accepts requests from the chart-studio frontend.

```
TILED_ALLOW_ORIGINS="https://chart-studio.plotly.com https://vega.github.io/voyager/" tiled serve pyobject tiled.examples.generic:demo
```

Navigate your browser to https://chart-studio.plotly.com. Use the "Import"
feature to import data by URL. Enter a URL such as
``http://localhost:8000/dataframe/full/dataframes/df?format=text/csv``.

Navigate your browser to https://vega.github.io/voyager/. Click "Load", "From URL",
set the file type to "JSON" and then use the same URL in the previous example.