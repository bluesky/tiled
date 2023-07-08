# Python Client

## Constructors

These are functions for constructing a client object.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.from_uri
   tiled.client.from_profile
```

## Client Container

The Container interface extends the ``collections.abc.Mapping`` (i.e. read-only
dict) interface, so it supports these standard "magic methods":

* `__getitem__` (lookup by key with `[]`)
* `__iter__` (iteration, use in for-loops for example)
* `__len__` (has a length, can be passed to `len`)

as well as:

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.container.Container.get
   tiled.client.container.Container.keys
   tiled.client.container.Container.items
   tiled.client.container.Container.values
```

The views returned by `.keys()`, `.items()`, and `.values()`
support efficient random access---e.g.

```py
c.values()[3]
c.values()[-1]
c.values()[:3]
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

Beyond the Mapping interface, Container adds the following attributes

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.container.Container.metadata
   tiled.client.container.Container.sorting
   tiled.client.container.Container.uri
   tiled.client.container.Container.specs
```

It adds these methods, which return a new Container instance.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.container.Container.search
   tiled.client.container.Container.sort
```

It adds this method, which returns the unique metadata keys,
structure_families, and specs of its children along with their counts.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.container.Container.distinct
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

## Context

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.context.Context
   tiled.client.context.Context.from_any_uri
   tiled.client.context.Context.from_app
   tiled.client.context.Context.authenticate
   tiled.client.context.Context.cache
   tiled.client.context.Context.force_auth_refresh
   tiled.client.context.Context.login
   tiled.client.context.Context.logout
   tiled.client.context.Context.tokens
```

## Cache

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.cache.Cache
   tiled.client.cache.Cache.clear
```
