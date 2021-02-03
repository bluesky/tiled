# Catalog Server from Scratch

## Requirements

* HTTP API that supports JSON and msgpack requests, with JSON and msgpack
  responses, as well as binary blob responses for chunked data
* Usable from ``curl`` and languages other than Python (i.e. support
  language-agnostic serialization options and avoid baking any Python-isms too
  deeply into the API)
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
* Usable performance without any instrisic caching in the server. Objects may
  do some internal caching for optimization, but the server will not explicitly
  hang on to any state between requests.
* Path toward adding state / caching in external systems (e.g. Redis, nginx)

## Draft Specification

There are two user-facing objects in the system, **Catalogs** and
**DataSources**. This specification proposes the Python API required to
Duck-will type as a Catalog or DataSource as well as a sample HTTP API
loosely based on [JSON API](https://jsonapi.org/), subject to future redesign.

This is a also a **registry of (de)serialization methods**
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
  implement one or both of these:

  ```python
  catalog.__len__
  catalog.__length_hint__
  ```

* The items in a Catalog MUST have an explicit and stable order.

* Catalogs MUST imlement an ``index`` attribute which supports efficient
  positional lookup and slicing for pagination. This always returns a Catalog of
  with a subset of the entries.

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

* Catalogs MUST implement a ``search`` method which returns another Catalog with
  a subset of the items. The signature of that method is intentionally not
  specified.

* Catalogs MUST implement a ``metadata`` attribute or property which
  returns a dict-like. This ``metadata`` is treated as user space, and no part
  of the server or client will rely on its contents.

* Catalogs MAY implement other methods beyond these for application-specific
  needs or usability.

* The method for initializing this object is intentionally unspecified. There
  will be variety.

* The data underlying the Catalog may be updated to add items, even though the
  Catalog itself is a read-only view on that data. Any items added MUST be added
  to the end. Items may not be removed.

### JSON API

List a Catalog to obtain its keys, paginated. It may contain subcatalogs or
datasources or a mixture.

```
GET /catalogs/keys/:path?page[offset]=50&page[limit]=5
```

```json
{
    "data": {
        "metadata": {},
        "catalogs":
            [
                {"key": "e370b080-c1ea-4db3-90d9-64a32e6de5a5", "links": {}},
                {"key": "50e81503-cdab-4370-8b0a-ce2ac192d20b", "links": {}},
                {"key": "cc868088-80fc-4876-9c9a-481a37420ceb", "links": {}},
                {"key": "5b13fd53-b6e4-410e-a310-2c1c31f10062", "links": {}},
                {"key": "0cd287ac-823c-4ed9-a008-2a68740e1939", "links": {}}
            ],
        "datasources": [],
    },
    "links": {
        "self": "...",
        "prev": "...",
        "next": "...",
        "first": "...",
        "last": "..."
    }
}
```

This is akin to ``list(catalog[path].index[offset:offset + limit])`` in the
Python API.

Get metadata for entries in a Catalog.

```
GET /catalogs/entries/:path?page[offset]=0&page[limit]=5
```

If it contains sub-catalogs, the response looks like:

```json
{
    "data": {
        "catalogs": [
            {"key": "...", "metadata": {}, "__qualname__": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "links": {}}
        ],
        "datasources": [],
    },
    "links": {
        "self": "...",
        "prev": "...",
        "next": "...",
        "first": "...",
        "last": "..."
    }
}
```

If it contains DataSources, the response looks like:

```json
{
    "data": {
        "catalogs": [],
        "datasources": [
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "links": {}}
        ],
    },
    "links": {
        "self": "...",
        "prev": "...",
        "next": "...",
        "first": "...",
        "last": "..."
    }
}
```

This is akin to
``[item.metadata for item in catalog[path].index[offset:offset + limit].values()]``
in the Python API.

Any of the above request may contain the query parameter ``q`` with a search
query. The responses have the same structure as above. This is equivalent to
``[item.metadata for item in catalog[path].search(query).index[offset:offset + limit].values()]``
in the Python API.

### DataSources

#### Python API

* DataSources MUST implement a ``metadata`` attribute or property which returns
  a dict-like. This ``metadata`` is treated as user space, and no part of the
  server or client will rely on its contents.

* DataSources MUST implement a ``container`` attribute or property which returns
  a string of the general type that will be returned by ``read()``, as in
  intake. These will be generic terms like ``"tabular"``, not the
  ``__qualname__`` of the class.

* DataSources MUST implement a method ``describe()`` with no arguments
  which returns a description sufficient to construct the container before
  fetching chunks of the data. The content of this description depends on the
  container. For example, it always includes the machine data type, and
  where applicable it includes shape, chunks, and a notion of high-level
  structure like columns, dimensions, indexes. This should include links to get
  the chunks with a range of available serializations.

* DataSources MUST implement a method ``read()`` with no arguments which returns
  the data structure.

* DataSources MAY implement other methods beyond these for application-specific
  needs or usability.

#### JSON API

We saw above how to list the DataSources in a Catalog, either just their key in
the Catalog or their their ``metadata`` and ``container`` as well. To
additionally include the output of ``describe()``:

```
GET /catalogs/descriptions/:path?page[offset]=0&page[limit]=5
```

```json
{
    "data": {
        "catalogs": [],
        "datasources": [
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}},
            {"key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}}
        ],
    },
    "links": {
        "self": "...",
        "prev": "...",
        "next": "...",
        "first": "...",
        "last": "..."
    }
}
```

To describe a single DataSource, give the path to the DataSource.
```
GET /datasource/description/:path
```

```json
{
    "data": {
        "key": "...", "metadata": {}, "__qualname__": "...", "container": "...", "description": {}, "links": {}
    },
    "links": {
        "self": "...",
        "prev": "...",
        "next": "...",
        "first": "...",
        "last": "..."
    }
}
```

The content of ``description`` provides information about how to specify
``chunk`` and a list of supported ``Content-Encoding`` headers for the request
for a blob of data. Examples may include a C buffer, Arrow, msgpack, or JSON.

```
GET /datasource/blob/:path?chunk=...
```

### Serialization Dispatch

This can closely follow how `dask.distributed` handles serialization. We may be
able to just reuse `dask.distributed`'s machinery, in fact. The important
difference is our choice of serializers. We do not need to serialize all of
Python; we need to serialize specific data structures and we need to do it in a
way that works for clients in languages other than Python.

see [dask.distributed serialization docs](https://distributed.dask.org/en/latest/serialization.html).
