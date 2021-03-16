# Draft Specification

There are four user-facing objects in the system:

* **Reader** -- extracts the metadata, structure, and (tiled) data from some
  source of store or generated data
* **Catalog** -- nestable collection of other Catalogs or Readers
* **Query** -- high-level description of a search query over entries in a
  Catalog
* **AccessPolicy** -- class with methods for enforcing access control on which
  entries in a Catalog a given identity (user) can see

This specification proposes the Python API required to duck-type as a Catalog or
Reader as well as a sample HTTP API based on
[JSON API](https://jsonapi.org/).

## Python API

### Readers

* Readers MUST implement a ``metadata`` attribute or property which returns
  a dict-like. This ``metadata`` is treated as user space, and no part of the
  server or client will rely on its contents.

* Readers MUST implement a ``container`` attribute or property which returns
  a string of the general type that will be returned by ``read()``, as in
  intake. These will be generic terms like ``"array"``, not the
  ``__qualname__`` of the class. It is meant to encompass the range of concrete
  types (cupy array, sparse array, dask array, numpy ndarray) that duck-type as
  a given generic container.

* Readers MUST implement a method ``structure()`` with no arguments
  which returns a description of the structure of this data. For each container
  (array, dataframe, etc.) there will be a specific schema for this description
  (TBD). For example, "array" reports machine data type, shape, and chunks.
  Richer structure (e.g. xarray) will include high-level structure like columns,
  dimensions, indexes.

* Readers MUST implement a method ``read()`` returns the data structure. It MAY
  return a lazy object, such as a PIMS sequence, dask array, dask dataframe, or
  other structure with delayed I/O.

* Readers MUST implement a method ``read_block(...)`` that efficiently reads and
  return a single block of data. It MUST perform all necessary I/O and return a
  self-contained and complete structure; that is, unlike ``read()`` it must not
  be delayed. The signature SHOULD reuse existing terminology from the
  corresponding data strucutre.  Examples:

  ```py
  array_reader.read_block(block=(0, 0))
  dataframe_reader.read_block(partition=0)
  dataset_reader.read_block(variable="image", coord="x", block=(0,))
  ```

  Additionally, the method SHOULD accept a ``slice`` parameter which takes a
  tuple of Python ``slice`` objects into the block to specify a partial block.

* Reader MAY allocate system resources (file handles, network connections, a
  cache in memory) but MUST implement a method ``close()`` which releases those
  resources. It MUST also implement the context manager API and call ``close()``
  on exiting the context.

* Readers MAY implement other methods beyond these for application-specific
  needs or usability.

### Catalogs

* Catalogs MUST implement the ``collections.abc.Mapping`` interface. That is:

  ```python
  catalog.__getitem__
  catalog.__iter__
  ```

  Catalogs may omit ``__len___`` as long as they provide
  [``__length_hint__``](https://www.python.org/dev/peps/pep-0424/), an estimated
  length that may be less expensive for Catalogs backed by databases. That is,
  implement one or both of these:

  ```python
  catalog.__len__
  catalog.__length_hint__
  ```

* Catalogs MUST implement an attributes which support efficient positional
  lookup and slicing.

  ```python
  catalog.keys_indexer[i]             # -> str
  catalog.values_indexer[i]           # -> Union[Catalog, Reader]
  catalog.items_indexer[i]            # -> Tuple[str, Union[Catalog, Reader]]
  catalog.keys_indexer[start:stop]    # -> List[str]
  catalog.items_indexer[start:stop]   # -> List[Union[Catalog, Reader]]
  catalog.values_indexer[start:stop]  # -> List[Tuple[str, Union[Catalog, Reader]]]
  ```

* It is NOT necessary to implemented strided slicing, as in ``[start:stop:5]``.
  It is sufficient to assume the stride is always 1.

* The values in a Catalog MUST be other Catalogs or Readers.

* The keys in a Catalog MUST be non-empty strings adhering to the JSON API spec
  for allowed characters in resource ids.

* Catalogs MUST implement a ``search`` method which accepts a ``Query`` as its
  argument and returns another Catalog with a subset of the items.  specified.

* Catalogs MUST implement a ``metadata`` attribute or property which
  returns a dict-like. This ``metadata`` is treated as user space, and no part
  of the server or client will rely on its contents.

* Catalogs MAY implement other methods beyond these for application-specific
  needs or usability.

* The method for initializing this object is intentionally unspecified. There
  will be variety.

* [This may need revisiting. Should it be qualified? Eliminated?] The items in a
  Catalog MUST have an explicit and stable order.

* [This may need revisiting. Should it be qualified? Eliminated?] The data
  underlying the Catalog may be updated to add items, even though the Catalog
  itself is a read-only view on that data. Any items added MUST be added to the
  end. Items may not be removed.

#### Queries

* Queries MUST be dataclasses.

* They MAY have any attributes. There are no required attributes.

#### Access Policy

An ``AcessPolicy`` is tightly coupled to:

1. How the metadata related to data management are stored --- "Which type of
   database? Which collection / table in the database? Which field / column in
   that table?"
2. The particulars of a given access management system that encodes who can see
   what --- "Given an authenticated identity, what API do I use to check what it
   is allowed to see?"

We envision a custom ``AccessPolicy`` will be needed for every combination of
(1) and (2). It is injected into a given ``Catalog`` at ``__init__`` time.

An ``AccessPolicy`` gets two opportunities to restrict the entries that a caller
can see. First, a Catalog gives it the opportunity to modify a query before it
is processed in order to restirct the set of results marshalled from storage.
Second, it may filter the query results before they are returned to the user.
Depending on the storage and the complexity of the data access rules, one or
both of these modes may be used by a given AccessPolicy.

* An AccessPolicy MUST implement a method
  ``modify_query(query: Query, authenticated_identity: str) -> Query``. This MAY
  filter the result at the database level so that less data is marshalled from
  storage.
* An AccessPolicy MUST implement a method
  ``filter_results(catalog, authenticated_identity: str) -> Catalog``
* An AccessPolicy MUST implement a method
  ``check_compatibility(catalog) -> bool`` which can be used by the Catalog to
  verify at its ``__init___`` time that the ``AccessPolicy`` it has been given
  understands how to modify its queries.
