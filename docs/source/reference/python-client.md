# Python Client

## Constructors

These are functions for constructing a client object.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.from_uri
   tiled.client.from_profile
```

## Client Node

The Node interface extends the ``collections.abc.Mapping`` (i.e. read-only
dict) interface, so it supports these standard "magic methods":

* `__getitem__` (lookup by key with `[]`)
* `__iter__` (iteration, use in for-loops for example)
* `__len__` (has a length, can be passed to `len`)

as well as:

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.get
   tiled.client.node.Node.keys
   tiled.client.node.Node.items
   tiled.client.node.Node.values
```

The views returned by `.keys()`, `.items()`, and `.values()`
support efficient random access---e.g.

```py
node.values()[3]
node.values()[-1]
node.values()[:3]
```

and several convenience methods:


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.iterviews.ValuesView.first
   tiled.iterviews.ValuesView.last
   tiled.iterviews.ValuesView.head
   tiled.iterviews.ValuesView.tail
```

Likewise for `.keys()` and `.items()`.

Beyond the Mapping interface, Node adds the following attributes

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.metadata
   tiled.client.node.Node.references
   tiled.client.node.Node.sorting
   tiled.client.node.Node.uri
   tiled.client.node.Node.specs
```

It adds these methods, which return a new Node instance.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.search
   tiled.client.node.Node.sort
```

It adds these methods for downloading and refreshing cached data.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.download
   tiled.client.node.Node.refresh
```

It adds this method, which returns the unique metadata keys,
structure_families, and specs of its children along with their counts.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.distinct
```

## Structure Clients

For each *structure family* ("array", "dataframe", etc.) there is a client
object that understand how to request and decode chunks/partitions of data
for this structure.

In fact, there can be *more than one* client for a given structure family.
Tiled currently includes two clients for each structure family:

* A client that reads the data into dask-backed objects (dask array, dask
  DataFrame, xarray objects backed by dask arrays)
* A client that reads the data into in-memory structures (numpy array, pandas
  DataFrame, xarray objects backed by numpy arrays)


### Base

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.base.BaseClient
   tiled.client.base.BaseClient.formats
   tiled.client.base.BaseClient.metadata
   tiled.client.base.BaseClient.uri
   tiled.client.base.BaseClient.item
   tiled.client.base.BaseClient.login
   tiled.client.base.BaseClient.logout
   tiled.client.base.BaseClient.new_variation
   tiled.client.base.BaseClient.specs
   tiled.client.base.BaseStructureClient.download
   tiled.client.base.BaseStructureClient.refresh
   tiled.client.base.BaseStructureClient.structure
```


### Array

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.array.DaskArrayClient
   tiled.client.array.DaskArrayClient.read_block
   tiled.client.array.DaskArrayClient.read
   tiled.client.array.DaskArrayClient.export
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.array.ArrayClient
   tiled.client.array.ArrayClient.read_block
   tiled.client.array.ArrayClient.read
   tiled.client.array.DaskArrayClient.export
```

### Sparse Array

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.sparse.SparseClient
   tiled.client.sparse.SparseClient.read
   tiled.client.sparse.SparseClient.export
```

### DataFrame

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.dataframe.DaskDataFrameClient
   tiled.client.dataframe.DaskDataFrameClient.read_partition
   tiled.client.dataframe.DaskDataFrameClient.read
   tiled.client.dataframe.DaskDataFrameClient.export
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.dataframe.DataFrameClient
   tiled.client.dataframe.DataFrameClient.read_partition
   tiled.client.dataframe.DataFrameClient.read
   tiled.client.dataframe.DaskDataFrameClient.export
```

### Xarray Dataset

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DaskDatasetClient
   tiled.client.xarray.DaskDatasetClient.read
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DatasetClient
   tiled.client.xarray.DatasetClient.read
```

## Cache

The module `tiled.client.cache` includes objects inspired by https://github.com/dask/cachey/

We opted for an independent implementation because reusing cachey would have required:

* An invasive subclass that could be a bit fragile
* And also composition in order to get the public API we want
* Carrying around some complexity/features that we do not use here

The original cachey license (which, like Tiled's, is 3-clause BSD) is included in
the same source directory as the `tiled.client.cache` module. (Cachey itself
*is* used in the server, where the use case is a better fit.)

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.cache.Cache
   tiled.client.cache.Cache.in_memory
   tiled.client.cache.Cache.on_disk
   tiled.client.cache.download
   tiled.client.cache.Scorer
```

## Context

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.context.Context
   tiled.client.context.Context.from_any_uri
   tiled.client.context.Context.from_app
   tiled.client.context.Context.authenticate
   tiled.client.context.Context.offline
   tiled.client.context.Context.force_auth_refresh
   tiled.client.context.Context.login
   tiled.client.context.Context.logout
   tiled.client.context.Context.tokens
```
