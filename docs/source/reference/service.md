# Service-side Components

## Readers

### Adapters

These "readers" don't do an I/O at all, but instead wrap a structure in memory
or its dask counterpart. They can be used to build other Readers.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.readers.array.ArrayAdapter
   tiled.readers.dataframe.DataFrameAdapter
   tiled.readers.xarray.VariableAdapter
   tiled.readers.xarray.DataArrayAdapter
   tiled.readers.xarray.DatasetAdapter
```

### File Readers

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.readers.dataframe.DataFrameAdapter.read_csv
   tiled.readers.tiff.TiffReader
   tiled.readers.excel.ExcelReader
   tiled.readers.hdf5.HDF5Reader
```

## Catalogs

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.catalogs.files.Catalog
   tiled.catalogs.in_memory.Catalog
   tiled.catalogs.utils
```

## Search Queries

### Built-in Search Query Types

These are simple, JSON-serializable dataclasses that define the *data*
in a query. They do not defined *how* to execute a query on a given Catalog.

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

   tiled.query_registration.register
   tiled.query_registration.name_to_query_type
   tiled.query_registration.query_type_to_name
```

## Media Type (Format) Registry

This is a registry of formats that the service can *write* upon a client's request.


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.media_type_registration.Registry
   tiled.media_type_registration.serialization_registry
```

## Structures

For each data structure supported by tiled, there are dataclasses that encode
its structure.  These are very lightweight objects; they are used to
inexpensively construct and a communicate a representation of the data's
shape and chunk/partition structure to the client so that it can formulate
requests for slices of data and decode the responses.

See {doc}`../explanations/structures` for more context.

### Array

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.array.ArrayStructure
   tiled.structures.array.ArrayMacroStructure
   tiled.structures.array.MachineDataType
   tiled.structures.array.Kind
   tiled.structures.array.Endianness
```

### DataFrame

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.dataframe.DataFrameStructure
   tiled.structures.dataframe.DataFrameMacroStructure
   tiled.structures.dataframe.DataFrameMicroStructure
```

### Xarray Structures

Xarrays are "meta" structures that contain chunks of arrays. For this reason,
they have no microstructure. Their macrostructure encompasses nested array
structures.

#### Variable

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.xarray.VariableStructure
   tiled.structures.xarray.VariableMacroStructure
```

#### DataArray

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.xarray.DataArrayStructure
   tiled.structures.xarray.DataArrayMacroStructure
```

#### Dataset

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.structures.xarray.DatasetStructure
   tiled.structures.xarray.DatasetMacroStructure
```

## Configuration Parsing

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.config.parse_configs
   tiled.config.direct_access
   tiled.config.direct_access_from_profile
   tiled.config.construct_serve_catalog_kwargs
```
## HTTP Server Application

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.server.app.serve_catalog
   tiled.server.app.get_app
```