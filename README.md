# Catalog Server from Scratch
*Disclaimer: This is very early work, presented in the spirit of an early draft
of an RFC.*

## Try the prototype

Install dependencies.

```
git clone https://github.com/danielballan/catalog-server-from-scratch
cd catalog-server-from-scratch
pip install -r requirements.tnt
pip install -e .
```
Run server.

```
uvicorn catalog_server.server:app --reload
```

Make requests. The server accepts JSON and msgpack. Once the server is running,
visit ``http://localhost:8000/docs`` for documentation. (Or, see below for
example requests and responses.)

The server serves a demo catalog by default, equivalent to:

```
ROOT_CATALOG="catalog_server.example_catalogs:catalog" uvicorn catalog_server.server:app --reload
```

Other catalogs can be served by changing the value of the `ROOT_CATALOG`
environment variable to point to a different object in any importable Python
module.

## Requirements

* HTTP API that supports JSON and msgpack requests, with JSON and msgpack
  responses, as well as binary blob responses for chunked data
* Be usable from ``curl`` and languages other that Python. In contrast to
  ``dask.distributed`` and the current ``intake-server`` avoid baking any
  Python-isms deeply into it. No pickle, no msgpack-python.
* List Runs, with pagination and random access
* Search Runs, with pagination and random access on search results
* Access Run metadata cheaply, again with pagination and random access.
* Access Run data as strided C arrays in chunks.
* A Python client with rich proxy objects that do chunk-based access
  transparently (like intake's `RemoteXarray` and similar). But, differently
  from current intake and Databroker, do not switch dask-vs-not-dask or
  dask-vs-another-delayed-framework at call time. Use a consistent delayed
  framework (or none at all) consistently within a given context. Your only
  option at call time should be `read()`. Whether that is in memory, dask, or
  something else should be set higher up.
* Usable performance without any intrinsic caching in the server. Objects may
  do some internal caching for optimization, but the server will not explicitly
  hang on to any state between requests.
* Path toward adding state / caching in external systems (e.g. Redis, nginx)

## Draft Specification

There are three user-facing objects in the system:

* **DataSource** -- wrapper around an array, dataframe, or other data container
* **Catalog** -- nestable collection of other Catalogs or DataSources
* **Query** -- high-level description of a search query over entries in a
  Catalog

This specification proposes the Python API required to duck-type as a Catalog or
DataSource as well as a sample HTTP API based on
[JSON API](https://jsonapi.org/).

### Python API

#### DataSources

* DataSources MUST implement a ``metadata`` attribute or property which returns
  a dict-like. This ``metadata`` is treated as user space, and no part of the
  server or client will rely on its contents.

* DataSources MUST implement a ``container`` attribute or property which returns
  a string of the general type that will be returned by ``read()``, as in
  intake. These will be generic terms like ``"array"``, not the
  ``__qualname__`` of the class. It is meant to encompass the range of concrete
  types (cupy array, sparse array, dask array, numpy ndarray) that duck-type as
  a given generic container.

* DataSources MUST implement a method ``describe()`` with no arguments
  which returns a description of the structure of this data. For each container
  (array, dataframe, etc.) there will be a specific schema for this description
  (TBD). For example, "array" reports machine data type, shape, and chunks.
  Richer structure (e.g. xarray) will include high-level structure like columns,
  dimensions, indexes.

* DataSources MUST implement a method ``read()`` with no arguments which returns
  the data structure.

* DataSources MAY implement other methods beyond these for application-specific
  needs or usability.

#### Catalogs

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
  catalog.values_indexer[i]           # -> Union[Catalog, DataSource]
  catalog.items_indexer[i]            # -> Tuple[str, Union[Catalog, Datasource]]
  catalog.keys_indexer[start:stop]    # -> List[str]
  catalog.items_indexer[start:stop]   # -> List[Union[Catalog, Datasource]]
  catalog.values_indexer[start:stop]  # -> List[Tuple[str, Union[Catalog, Datasource]]]
  ```

* The values in a Catalog MUST be other Catalogs or DataSources.

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

#### Extension points

The prototype uses several user-configurable registries for extensibility of
various features.

* **MIME types for data** For each container type (e.g. "array") there is a
  registry mapping MIME type (e.g. `application/octet-stream`,
  `application/json`) to a function that can encode a block of data from that
  container.
* **Query types** The server and client use a registry of associates each
  `Query` with a string name. Additional queries can be registered.
* **Query support for a given Catalog type** In the method `Catalog.search`, a
  Catalog needs to translate the generic *description* encoded by a `Query` into
  a concrete filtering operation on its particular storage backend. Thus, custom
  Queries also need to registered by Catalogs. It is not necessary for every
  Catalog to understand every type of Query.


### JSON API

Examples from the prototype....

List entries in the root catalog, paginated.

```
GET /entries?page[offset]=2&page[limit]=2
```

```json
{
    "data": [
        {
            "attributes": {
                "count": 3,
                "metadata": {
                    "animal": "dog",
                    "fruit": "orange"
                }
            },
            "id": "medium",
            "meta": {
                "__module__": "catalog_server.in_memory_catalog",
                "__qualname__": "Catalog"
            },
            "type": "catalog"
        },
        {
            "attributes": {
                "count": 3,
                "metadata": {
                    "animal": "penguin",
                    "fruit": "grape"
                }
            },
            "id": "large",
            "meta": {
                "__module__": "catalog_server.in_memory_catalog",
                "__qualname__": "Catalog"
            },
            "type": "catalog"
        }
    ],
    "error": null,
    "links": {
        "first": "/?page[offset]=0&page[limit]=2",
        "last": "/?page[offset]=2&page[limit]=2",
        "next": null,
        "prev": "/?page[offset]=0&page[limit]=2",
        "self": "/?page[offset]=2&page[limit]=2"
    },
    "meta": {
        "count": 4
    }
}
```

Search the full text of the metadata of the entries in the root catalog.

```
GET /search?filter[fulltext][condition][text]=dog
```

```json
{
    "data": [
        {
            "attributes": {
                "count": 3,
                "metadata": {
                    "animal": "dog",
                    "fruit": "orange"
                }
            },
            "id": "medium",
            "meta": {
                "__module__": "catalog_server.in_memory_catalog",
                "__qualname__": "Catalog"
            },
            "type": "catalog"
        }
    ],
    "error": null,
    "links": {
        "first": "/?page[offset]=0&page[limit]=10",
        "last": "/?page[offset]=0&page[limit]=10",
        "next": null,
        "prev": null,
        "self": "/?page[offset]=0&page[limit]=10"
    },
    "meta": {
        "count": 1
    }
}
```

View the metadata of a sub-Catalog.

```
GET /metadata/tiny
```

```json
{
    "data": {
        "attributes": {
            "count": 3,
            "metadata": {
                "animal": "bird",
                "fruit": "apple"
            }
        },
        "id": "tiny",
        "meta": {
            "__module__": "catalog_server.in_memory_catalog",
            "__qualname__": "Catalog"
        },
        "type": "catalog"
    },
    "error": null,
    "links": null,
    "meta": null
}
```

List the entries of a sub-Catalog. In this case, these are "array" DataSources.
We are given their machine datatype, shape, and chunk strucutre

```
GET /entries/tiny
```

```json
{
    "data": [
        {
            "attributes": {
                "metadata": {},
                "structure": {
                    "chunks": [
                        [
                            3
                        ],
                        [
                            3
                        ]
                    ],
                    "dtype": {
                        "endianness": "little",
                        "itemsize": 8,
                        "kind": "f"
                    },
                    "shape": [
                        3,
                        3
                    ]
                }
            },
            "id": "ones",
            "meta": {
                "__module__": "catalog_server.datasources",
                "__qualname__": "ArraySource"
            },
            "type": "datasource"
        },
        {
            "attributes": {
                "metadata": {},
                "structure": {
                    "chunks": [
                        [
                            3
                        ],
                        [
                            3
                        ]
                    ],
                    "dtype": {
                        "endianness": "little",
                        "itemsize": 8,
                        "kind": "f"
                    },
                    "shape": [
                        3,
                        3
                    ]
                }
            },
            "id": "twos",
            "meta": {
                "__module__": "catalog_server.datasources",
                "__qualname__": "ArraySource"
            },
            "type": "datasource"
        },
        {
            "attributes": {
                "metadata": {},
                "structure": {
                    "chunks": [
                        [
                            3
                        ],
                        [
                            3
                        ]
                    ],
                    "dtype": {
                        "endianness": "little",
                        "itemsize": 8,
                        "kind": "f"
                    },
                    "shape": [
                        3,
                        3
                    ]
                }
            },
            "id": "threes",
            "meta": {
                "__module__": "catalog_server.datasources",
                "__qualname__": "ArraySource"
            },
            "type": "datasource"
        }
    ],
    "error": null,
    "links": {
        "first": "/?page[offset]=0&page[limit]=10",
        "last": "/?page[offset]=0&page[limit]=10",
        "next": null,
        "prev": null,
        "self": "/?page[offset]=0&page[limit]=10"
    },
    "meta": {
        "count": 3
    }
}
```

Get a chunk of data from the array as JSON (using a small array as an example)
and as binary (using a large array as an example).

```
GET /blob/array/tiny/threes?block=0,0 Accept:application/json
```

```json
[
    [
        3.0,
        3.0,
        3.0
    ],
    [
        3.0,
        3.0,
        3.0
    ],
    [
        3.0,
        3.0,
        3.0
    ]
]
```

```
GET /blob/array/large/threes?block=0,0 Accept:application/octet-stream
```

```
<50000000 bytes of binary data>
```
