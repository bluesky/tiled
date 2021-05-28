# Python Client

## Constructors

These are functions for constructing a client object.

### Standard constructors

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.from_uri
   tiled.client.from_profile
```

### Special constructors

These are typically used for development and debugging only.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.from_catalog
   tiled.client.from_config
   tiled.client.from_client
```

## Client Catalog

The Catalog interface extends the ``collections.abc.Mapping`` (i.e. read-only
dict) interface, so it supports these standard "magic methods":

* `__getitem__` (lookup with `[]`)
* `__iter__` (iteration, use in for-loops for example)
* `__len__` (has a length, can be passed to `len`)

as well as:

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.catalog.Catalog.get
   tiled.client.catalog.Catalog.keys
   tiled.client.catalog.Catalog.items
   tiled.client.catalog.Catalog.values
```

It adds the following attributes

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.catalog.Catalog.metadata
   tiled.client.catalog.Catalog.sorting
   tiled.client.catalog.Catalog.path
   tiled.client.catalog.Catalog.uri
```

And it adds these methods, which return a new Catalog instance.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.catalog.Catalog.search
   tiled.client.catalog.Catalog.sort
```

Finally, it adds attributes that provide efficient positional-based lookup,
as in ``catalog.values_indexer[500:600]``.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.catalog.Catalog.keys_indexer
   tiled.client.catalog.Catalog.items_indexer
   tiled.client.catalog.Catalog.values_indexer
```

Finally, it exposes these methods, which are used internally and may be useful
in advanced applications.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.catalog.Catalog.client_for_item
   tiled.client.catalog.Catalog.new_variation
   tiled.client.catalog.Catalog.touch
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
   tiled.client.base.BaseClient.metadata
   tiled.client.base.BaseClient.path
   tiled.client.base.BaseClient.uri
   tiled.client.base.BaseClient.username
   tiled.client.base.BaseClient.item
   tiled.client.base.BaseClient.new_variation
   tiled.client.base.BaseStructureClient.touch
   tiled.client.base.BaseStructureClient.structure
   tiled.client.base.BaseArrayClient.structure
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

### Xarray Structures

#### Variable

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DaskVariableClient
   tiled.client.xarray.DaskVariableClient.read_block
   tiled.client.xarray.DaskVariableClient.read
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.VariableClient
   tiled.client.xarray.VariableClient.read_block
   tiled.client.xarray.VariableClient.read
```

#### DataArray

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DaskDataArrayClient
   tiled.client.xarray.DaskDataArrayClient.coords
   tiled.client.xarray.DaskDataArrayClient.read_block
   tiled.client.xarray.DaskDataArrayClient.read
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DataArrayClient
   tiled.client.xarray.DataArrayClient.coords
   tiled.client.xarray.DataArrayClient.read_block
   tiled.client.xarray.DataArrayClient.read
```

#### Dataset

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DaskDatasetClient
   tiled.client.xarray.DaskDatasetClient.coords
   tiled.client.xarray.DaskDatasetClient.data_vars
   tiled.client.xarray.DaskDatasetClient.read
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.xarray.DatasetClient
   tiled.client.xarray.DatasetClient.data_vars
   tiled.client.xarray.DatasetClient.coords
   tiled.client.xarray.DatasetClient.read
```

## Cache

The module `tiled.client.cache` includes objects inspired by https://github.com/dask/cachey/

We opted for an independent implementation because reusing cachey would have required:

* An invasive subclass that could be a bit fragile
* And also composition in order to get the public API we want
* Carrying around some complexity/features that we do not use here

The original cachey license (which, like Tiled's, is 3-clause BSD) is included in
the same source directory as the `tiled.client.cache` module.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.cache.Cache
   tiled.client.cache.Cache.in_memory
   tiled.client.cache.Cache.on_disk
   tiled.client.cache.download
   tiled.client.cache.Scorer
```