# HTTP API Overview

To view and try the interactive docs, start the Tiled server with the demo
Catalog from a Terminal

```
tiled serve pyobject --public tiled.examples.generated:demo
```

and navigate your browser to http://localhost:8000/docs.

## Big Picture

The API is based on the [JSON API standard](https://jsonapi.org/), but no
firm decision has been made yet about whether this is a good fit for Tiled.

The routes are generally spelled like ``/{action}/{path}/``, like GitHub
repository URLs, with the path following the structure of the Catalog
entries.

The ``GET /entries`` route lists Catalogs and provides configurable subset of
the metadata about each entry. The ``GET /search`` route provides a subset of
the entries matching a query.

The ``GET /metadata`` route provides the metadata about one entry.

The data access routes like ``GET /array/block`` and ``GET /array/full`` are
designed to different kinds of clients. Both support slicing / sub-selection
as appropriate to the data structure. Generic clients, like a web browser,
should use the "full" route, which sends the entire (sliced) result in one
response. More sophisticated clients that can reassemble tiled results should
use the other routes, which support efficient chunk-based access.