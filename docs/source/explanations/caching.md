# Caches

## Overview

Caching can make Tiled faster. Because, in general, caches make programs more
complex and harder to trace, Tiled was designed without any caching at first.
Caches were added with clear separation from the rest of Tiled and an easy
opt-out path.

Tiled has two kinds of caching:

1. **Client-side response cache.** The Tiled Python client implements a standard
   web cache, similar in both concept and implementation to a web browser's cache.
3. **Service-side object cache.** The _response_ caches operate near the outer
   edges of the application, stashing and retrieve HTTP response bytes. The
   _object_ cache is more deeply integrated into the application: it is
   available for authors of Adapters to use for stashing any objects that may be
   useful in expediting future work. These objects may serializable, such as chunks
   of array data, or unserializable, such as file handles. Requests that ask for
   overlapping but distinct slices of data or requests that ask for the same
   data but in varied formats will not benefit from the _response_ cache; they
   will "miss". The _object_ cache, however, can slice and encode its cached
   resources differently for different requests. The object cache will not provide
   quite the same speed boost as a response cache, but it has a broader impact.

(client-http-response-cache)=
## Client-side HTTP Response Cache

The client response cache is an LRU response cache backed by a SQLite file.


```py
from tiled.client import from_uri

client = from_uri("...")
client.context.cache.clear()  # clear cache
c.context.cache.filepath  # locate SQLite file

# Customize the cache.

from tiled.client.cache import Cache

cache = Cache(
    capacity=500_000_000,  # bytes
    max_item_size=500_000,  # bytes
    filepath="path/to/my_cache.db",
    readonly=False,
)
```

## Server-side Object Cache

TO DO

###  Connection to Dask

Dask provides an opt-in, experimental
[opportunistic caching](https://docs.dask.org/en/latest/caching.html) mechanism.
It caches at the granularity of "tasks", such as chunks of array or partitions
of dataframes.

Tiled's object cache is generic---not exclusive to dask code paths---but it plugs
into dask in a similar way to make it easy for any Adapters that happen to use
dask to leverage Tiled's object cache very simply, like this:

```py
from tiled.server.object_cache import get_object_cache


with get_object_cache().dask_context:
    # Any tasks that happen to already be cached will be looked up
    # instead of computed here. Anything that _is_ computed here may
    # be cached, depending on its bytesize and its cost (how long it took to
    # compute).
    dask_object.compute()
```

Items can be proactively cleared from the cache like so:

```py
from tiled.server.object_cache import get_object_cache, NO_CACHE


cache = get_object_cache()
if cache is not NO_CACHE:
    cache.discard_dask(dask_object.__dask_keys__())
```
