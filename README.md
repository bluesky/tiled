# Catalog Server from Scratch

## Requirements

* HTTP API that supports JSON requests, with JSON and binary responses
* Usable from ``curl`` and languages other than Python (i.e. support
  language-agnostic serialization options and avoid baking any Python-isms too
  deeply into the API)
* List Runs, with random paginated access
* Search Runs, with random paginated access on search results
* Access Run metadata cheaply, again with random paginated access.
* Access Run data as strided C arrays in chunks.
* A Python client with rich proxy objects that do chunk-based access
  transparently using dask (like intake's `RemoteXarray` and similar)
* Usable performance without any instrisic caching in the server. Objects may
  do some internal caching for optimization, but the server will not explicitly
  hang on to any state between requests.
* Path toward adding state / caching in external systems (e.g. Redis, nginx)

## Draft Specification

There are two user-facing objects in the system, **Catalogs** and
**DataSources**. This is a also a **registry of (de)serialization methods**
single-dispatched on type, following ``dask.distributed``.

### Catalogs

#### Python API

* Catalogs MUST implement the ``collections.abc.Mapping`` interface. That is:

  ```python
  catalog.__getitem__
  catalog.__iter__
  ```

  Catalogs may omit ``__len___`` as long as they provide
  [``__length_hint__``](https://www.python.org/dev/peps/pep-0424/), an estimated
  length that may be less expensive for Catalogs backed by databases. That is,
  implement at this one of these:

  ```python
  catalog.__len__
  catalog.__length_hint__
  ```

* Catalogs MUST imlement an ``index`` attribute which supports efficient
  positional lookup and slicing for pagination.

  ```python
  catalog.index[i]
  catalog.index[start:stop]
  catalog.index[start:stop:stride]
  ```

  Support for strides other than ``1`` is optional. Support for negative indexes
  is optional. A ``NotImplementedError`` should be raised when a stride is not
  supported.

* The values in a Catalog MUST be other Catalogs or DataSources.

* The keys in a Catalog MUST be strings.

* Catalogs MUST implement a ``search`` method which returns another Catalog
  of the same type with a subset of the items. The signature of that method is
  intentionally not specified (but maybe it should be?).

* Catalogs MUST implement a ``metadata`` attribute or property which
  returns a dict-like. This ``metadata`` is treated as user space, and no part
  of the server or client will rely on its contents.

* Catalogs MAY implement other methods beyond these for application-specific
  needs or usability.

* The method for initializing this object is intentionally unspecified. There
  will variety.

### JSON API

List a Catalog to obtain its keys, paginated.

```
GET /list/:path?page[offset]=50&page[limit]=5
```

```json
{
    "data":
        [
            "e370b080-c1ea-4db3-90d9-64a32e6de5a5"
            "50e81503-cdab-4370-8b0a-ce2ac192d20b"
            "cc868088-80fc-4876-9c9a-481a37420ceb"
            "5b13fd53-b6e4-410e-a310-2c1c31f10062"
            "0cd287ac-823c-4ed9-a008-2a68740e1939"
        ],
    "links": {
        "prev": "..."
        "next": "..."
        "first": "..."
        "last": "..."
    }
}
```

This is akin to ``list(catalog[path].index[offset:offset + limit])`` in the
Python API.

Get metadata for entries in a Catalog.

```
GET /metadata/:path?page[offset]=0&page[limit]=5
```

```json
{
    "data":
        [
            {"metadata": {...}, "__qualname__": "..."},
            {"metadata": {...}, "__qualname__": "..."},
            {"metadata": {...}, "__qualname__": "..."},
            {"metadata": {...}, "__qualname__": "..."},
            {"metadata": {...}, "__qualname__": "..."}
        ]
    "links": {
        "prev": "..."
        "next": "..."
        "first": "..."
        "last": "..."
    }
}
```

This is akin to
``[item.metadata for item in catalog[path].index[offset:offset + limit].values()]``
in the Python API.

### DataSources

#### Python API

* DataSources MUST implement a method ``read()`` with no required arguments
  which returns the data.

* DataSources MUST implement a ``container`` attribute or property which returns
  a dict-like with the following contents:

  ```
  {"__qualname__": qualname of class that will be return by read()
  ```
* DataSources MUST implement a ``metadata`` attribute or property which returns a
  dict-like. This ``metadata`` is treated as user space, and no part of the
  server or client will rely on its contents.

* DataSources MAY implement other methods beyond these for application-specific
  needs or usability.

#### JSON API

### Serialization Dispatch

This can closely follow how `dask.distributed` handles serialization. We may be
able to just reuse `dask.distributed`'s machinery, in fact. The important
difference is our choice of serializers. We do not need to serialize all of
Python; we need to serialize specific data structures and we need to do it in a
way that works for clients in languages other than Python.

see [dask.distributed serialization docs](https://distributed.dask.org/en/latest/serialization.html).
