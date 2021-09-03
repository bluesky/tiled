# Caching Design and Roadmap

For practical guides on client-side and service-side caching, see
{doc}`../tutorials/caching` and {doc}`../how-to/tune-caches`.

```{note}
This page discusses both current features and planned features. Italicized remarks
in the discussion below makes clear what exists now and what is on the road map.
```

## Overview

There are three types of caches in Tiled:

1. **Client-side response cache.** The Tiled Python client implements a standard
   web cache, similar in both concept and implementation to a web browser's cache.
   It enables an offline "airplane mode". If a server is available, it enables the
   client to check whether the version it has is the latest one.
2. **Service-side response cache.**
   _This is not yet implemented, but planned soon._  This is also a standard web
   cache, on the server side. It stores the content of the most frequent responses.
   This covers use cases such as, "Several users are asking for the exact same
   chunks of data in the exact same format."
3. **Service-side data cache.** The _response_ caches are very finely targeted.
   Requests that ask for overlapping but distinct slices of data, or requests
   that ask for the same but in varied formats, will not benefit from that
   cache; they will "miss". The _data_ cache, however, stores chunks of (array,
   dataframe) data, which is it can slice and encode differently for different
   requests. It will not provide quite the same speed boost as a response cache,
   but it has a broader impact.

## Where is the cache content stored?

Caches can be _private_, stored in the memory space of the worker process,
or _shared_ via an external service like Redis, available to multiple workers
in a horizontally-scaled deployment.

The Tiled Python client currently supports a private, transient cache in memory
and a shared, persistent cache backed by files on disk. (The disk cache uses
file-based locking to ensure consistency.) The caching mechanism is pluggable:
other storage mechanisms can be injected without changes to Tiled itself.

Tiled plans to support placing the service-side caches---items (2) and (3)
above---in either private or shared mode, using respectively worker memory or
Redis. Different choices will give different benefits, as discussed below.
_Currently, only (3) is implemented and it only supports storage in worker
memory._

When the data cache (3) is private, it can simply store chunks of data (numpy
arrays, dataframe partitions, etc.) as live runtime objects in Python process
memory. Storage and retrieval are extremely cheap: they cost about one dictionary
lookup.  When the data cache (3) is shared, the data will have to be serialized
(e.g. with pickle) and moved into the shared external service. The benefit is
that, in a horizontally-scaled deployment with a user's requests load-balanced
over many workers, all workers will have fast access to the cached chunks of
data. The access will not be as fast in the private mode, but it will be much
faster than reading data from disk.

The entries in the response caches---items (1) and (2) above---are serialized
bytes (HTTP responses). Therefore, the overhead of moving them into external
storage is lower, as there is no serialization/deserialization step.

In the simple case of a single-worker deployment, for "scaled down" use on a
laptop, there is no speed benefit to keeping anything out-of-process, so Tiled
will always support the _option_ of placing (2) and (3) in worker memory, even
if shared memory proves to be a better choice for larger deployments.

## Connection to Dask

Dask provides an opt-in, experimental
[opportunistic caching](https://docs.dask.org/en/latest/caching.html) mechanism.
It caches at the granularity of "tasks", such as chunks of array or partitions
of dataframes.

Tiled's data cache is generic---not exclusive to dask code paths---but it plugs
into dask in a similar way to make it easy for any Adapters that happen to use
dask to leverage Tiled's data cache very simply, like this:

```py
from tiled.server.data_cache import get_data_cache


with get_data_cache().dask_context:
    # Any tasks that happen to already be cached will be looked up
    # instead of computed here. Anything that _is_ computer here may
    # be cached, depending on its bytesize and its cost (how long it took to
    # compute).
    dask_object.compute()
```

Items can be proactively clearly from the cache like so:

```py
from tiled.server.data_cache import get_data_cache, NO_CACHE


cache = get_data_cache()
if cache is not NO_CACHE:
    cache.discard_dask(dask_object.__dask_keys__())
```
