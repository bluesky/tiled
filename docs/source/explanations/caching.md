# Caching Design and Roadmap

For practical guides on client-side and service-side caching, see
{doc}`../tutorials/caching` and {doc}`../how-to/tune-caches`.

```{note}
This page discusses both current features and planned features. Italicized remarks
in the discussion below makes clear what exists now and what is on the road map.
```

## Overview

Caching can make Tiled faster. Because, in general, caches make programs more
complex and harder to trace, Tiled was designed without any caching at first.
Caches were added with clear separation from the rest of Tiled and an easy
opt-out path.

There are three types of centrally-managed cache in Tiled:

1. **Client-side response cache.** The Tiled Python client implements a standard
   web cache, similar in both concept and implementation to a web browser's cache.
   It enables an offline "airplane mode". If a server is available, it enables the
   client to inexpensively check whether the version it has is the latest one.
2. **Service-side response cache.**
   _This is not yet implemented, but planned soon._  This is also a standard web
   cache on the server side. It stores the content of the most frequent responses.
   This covers use cases such as, "Several users are asking for the exact same
   chunks of data in the exact same format."
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

## Where is the cache content stored?

Caches can be _private_, stored in the memory space of the worker process,
or _shared_ by multiple workers in a horizontally-scaled deployment via an
external service such as Redis.

The Tiled Python client currently supports a private, transient cache in memory
and a shared, persistent cache backed by files on disk. (The disk cache uses
file-based locking to ensure consistency.) The caching mechanism is pluggable:
other storage mechanisms can be injected without changes to Tiled itself.

On the service side, only the object cache is currently implemented, and it
currently supports storage in worker memory only. Workers cannot currently
access resources cached by other workers. In the future, Tiled will support
(optionally) configuring the service-side response and object caches to sync
with a shared Redis cache. Response data, being bytes, is straightforward to
stored in a shared cache. But only a subset of the items in the object
cache---those with known types and secure serialization schemes---will be
eligible for the shared cache. For example, Tiled cannot place a file handle in
Redis, and Tiled will not place unsigned pickled data in Redis (for security
reasons).

## Tiered private/shared caching

The planned syncing with a shared service-side cache will operate as follows:

* Worker A needs a resource. It checks its in-process cache and the shared cache,
  and it does not find it.
* Worker A creates the resource (e.g. a chunk of array data). It places a reference
  in its in-process cache. (This is very cheap.) It _also_ places a copy into the
  shared Redis cache. This requires serialization and transport over the network.
* Worker A needs the resource again. It finds it in its in-process cache.
* Worker B needs the same resource. It checks its in-process cache and does not find
  it there. It checks the shared cache and does find it. It loads the data
  from the shared cache. This requires network transport and deserialization, but
  it is (likely) much cheaper that reading from disk. Worker B may place a
  reference in its in-process cache so that the next access will be faster.
  If Worker B's in-process cache is near its maximum capacity, it will decide
  whether it is worthwhile to evict one of its existing items to make room for
  this latest one.
* Sometime later, both of these workers need the resource again and finds that
  the resource is no longer in their respective in-process caches---it has been
  evicted to make room for more frequently-used items. They find it in the shared
  cache. Again, they may restore it back into their in-process caches.
* Worker A processes a user request that _changes_ the resource and invalidates the
  cached data. It evicts the stale data from its in-process cache and from the
  shared cache. It announces---via a publish/subscribe mechanism---that the
  item is stale. Worker B receives this announcement and evicts the stale data
  from its in-process cache as well.  When the data is next accessed from either
  worker, it will be loaded fresh.
* Suppose Worker C also held a copy of this cached stale data in its in-process
  cache. And suppose it has momentarily lost its connection to the
  publish/subscribe mechanism and missed this announcement. Therefore, it is
  momentarily unaware that it is holding stale data. Will it ever get back in
  sync? The announcements include a incrementing counter, so that whenever the
  _next_ announcement is published or when Worker C performs a periodic
  check of the current counter value, Worker C will observe that it has missed
  one of more updates, and it will purge its in-process cache.

## Connection to Dask

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

## What other kinds of caching happen in Tiled?

The file-based directory-walking tree uses LRU caches, fixed at 10k items
per subdirectory, to stash Adapter instances on first access. It discards them
if the underlying file is removed or modified.
