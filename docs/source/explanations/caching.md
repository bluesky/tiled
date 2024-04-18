# Caches

## Overview

Caching can make Tiled faster. Because, in general, caches make programs more
complex and harder to trace, Tiled was designed without any caching at first.
Caches were added with clear separation from the rest of Tiled and an easy
opt-out path.

Tiled has two kinds of caching:

1. **Client-side response cache.** The Tiled Python client implements a standard
   web cache, similar in both concept and implementation to a web browser's cache.
2. **Server-side resource cache.** The resource cache is used to cache file
   handles and related system resources, to avoid rapidly opening, closing,
   and reopening the same files while handling a burst of requests.

(client-http-response-cache)=
## Client-side HTTP Response Cache

The client response cache is an LRU (Least Recently Used) response cache backed by a SQLite file.


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

## Server-side Resource Cache

The "resource cache" is a TLRU (Time-aware Least Recently Used) cache. When
items are evicted from the cache, a hard reference is dropped, freeing the
resource to be closed by the garbage collector if there are no other extant
hard references. Items are evicted if:

- They have been in the cache for a _total_ of more than a given time.
  (Accessing an item does not reset this time.)
- The cache is at capacity and this item is the least recently used item.

It is not expected that users should need to tune this cache, except in
debugging scenarios. These environment variables may be set to tune
the cache parameters:

```sh
TILED_RESOURCE_CACHE_MAX_SIZE  # default 1024 items
TILED_RESOURCE_CACHE_TTU  # default 60. seconds
```

The "size" is measured in cached items; that is, each item in the cache has
size 1.

To disable the resource cache, set:

```sh
TILED_RESOURCE_CACHE_MAX_SIZE=0
```

It is also possible to register a custom cache:

```python
from cachetools import Cache
from tiled.adapters.resource_cache import set_resource_cache

cache = Cache(maxsize=1)
set_resource_cache(cache)
```

Any object satisfying the `cachetools.Cache` interface is acceptable.
