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

   tiled.client.from_tree
   tiled.client.from_config
   tiled.client.from_context
```

## Client Node

The Node interface extends the ``collections.abc.Mapping`` (i.e. read-only
dict) interface, so it supports these standard "magic methods":

* `__getitem__` (lookup with `[]`)
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

It adds the following attributes

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.metadata
   tiled.client.node.Node.sorting
   tiled.client.node.Node.path
   tiled.client.node.Node.uri
```

And it adds these methods, which return a new Node instance.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.search
   tiled.client.node.Node.sort
```

Finally, it adds attributes that provide efficient positional-based lookup,
as in ``tree.values_indexer[500:600]``.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.keys_indexer
   tiled.client.node.Node.items_indexer
   tiled.client.node.Node.values_indexer
```

Finally, it exposes these methods, which are used internally and may be useful
in advanced applications.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.node.Node.client_for_item
   tiled.client.node.Node.new_variation
   tiled.client.node.Node.touch
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
   tiled.client.base.BaseClient.metadata tiled.client.base.BaseClient.path tiled.client.base.BaseClient.uri tiled.client.base.BaseClient.username
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
   tiled.client.array.DaskArrayClient.formats
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.array.ArrayClient
   tiled.client.array.ArrayClient.read_block
   tiled.client.array.ArrayClient.read
   tiled.client.array.DaskArrayClient.export
   tiled.client.array.DaskArrayClient.formats
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

## Context

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.client.context.Context
   tiled.client.context.Context.get_json
   tiled.client.context.Context.get_content
   tiled.client.context.Context.authenticate
   tiled.client.context.Context.reauthenticate
   tiled.client.context.Context.offline
   tiled.client.context.Context.base_url
   tiled.client.context.Context.path_parts
```