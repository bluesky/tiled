# Service-side Components

(adapters-ref)=
## Adapters

### Python Object Adapters

These Adapters don't do any I/O, but instead wrap a structure in memory
or its dask counterpart.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.adapters.mapping.MapAdapter
   tiled.adapters.array.ArrayAdapter
   tiled.adapters.table.TableAdapter
   tiled.adapters.sparse.COOAdapter
   tiled.adapters.xarray.DatasetAdapter.from_dataset
```

### File Adapters

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.adapters.csv.read_csv
   tiled.adapters.excel.ExcelAdapter
   tiled.adapters.hdf5.HDF5Adapter
   tiled.adapters.netcdf.read_netcdf
   tiled.adapters.parquet.ParquetDatasetAdapter
   tiled.adapters.sparse_blocks_parquet.SparseBlocksParquetAdapter
   tiled.adapters.tiff.TiffAdapter
   tiled.adapters.zarr.ZarrArrayAdapter
   tiled.adapters.zarr.ZarrGroupAdapter
```

## Search Queries

### Built-in Search Query Types

These are simple, JSON-serializable dataclasses that define the *data*
in a query. They do not defined *how* to execute a query on a given Tree.

```{note}
The list of built-in queries is short. Most of the power of queries comes from
registering *custom* queries that fit your use case and can make specific
assumption about your metadata / data and its meaning.
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.FullText
   tiled.queries.KeyLookup
```

### Custom Search Query Registration

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.query_registration.QueryRegistry
   tiled.query_registration.register
```

(media-type-registry-ref)=
## Media Type (Format) Registry

This is a registry of formats that the service can *write* upon a client's request.

When registering new types, make reference to the
[IANA Media Types (formerly known as MIME types)](https://www.iana.org/assignments/media-types/media-types.xhtml).

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.media_type_registration.serialization_registry
   tiled.media_type_registration.SerializationRegistry
   tiled.media_type_registration.SerializationRegistry.register
   tiled.media_type_registration.SerializationRegistry.media_types
   tiled.media_type_registration.SerializationRegistry.aliases
```

## Structures

For each data structure supported by tiled, there are dataclasses that encode
its structure.  These are very lightweight objects; they are used to
inexpensively construct and a communicate a representation of the data's
shape and chunk/partition structure to the client so that it can formulate
requests for slices of data and decode the responses.

See {doc}`../explanations/structures` for more context.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.array.ArrayStructure
   tiled.structures.array.BuiltinDtype
   tiled.structures.array.Endianness
   tiled.structures.array.Kind
   tiled.structures.core.Spec
   tiled.structures.core.StructureFamily
   tiled.structures.table.TableStructure
   tiled.structures.sparse.COOStructure
```

## Configuration Parsing

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.config.parse_configs
   tiled.config.construct_build_app_kwargs
```
## HTTP Server Application

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.server.app.build_app
   tiled.server.app.build_app_from_config
```

## Object Cache

The "object" cache is available to all Adapters to cache any objects, including
serializable objects like array chunks and unserializable objects like file
handles. It is a process-global singleton.

Implementation detail: It is backed by [Cachey](https://github.com/dask/cachey).

Adapters that use the cache _must_ use a tuple of strings and/or numbers as a
cache key and _should_ use a cache key of the form `(class.__module__,
class.__qualname__, ...)` to avoid collisions with other Adapters. See
`tiled.adapters.tiff` for a generic example and see `tiled.adapters.table` for
an example that uses integration with dask.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.server.object_cache.get_object_cache
   tiled.server.object_cache.set_object_cache
   tiled.server.object_cache.ObjectCache
   tiled.server.object_cache.ObjectCache.available_bytes
   tiled.server.object_cache.ObjectCache.get
   tiled.server.object_cache.ObjectCache.put
   tiled.server.object_cache.ObjectCache.discard
   tiled.server.object_cache.ObjectCache.clear
   tiled.server.object_cache.ObjectCache.dask_context
   tiled.server.object_cache.ObjectCache.discard_dask
```
