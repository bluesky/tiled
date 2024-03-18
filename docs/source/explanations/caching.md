# Caches

## Overview

Caching can make Tiled faster. Because, in general, caches make programs more
complex and harder to trace, Tiled was designed without any caching at first.
Caches were added with clear separation from the rest of Tiled and an easy
opt-out path.

Tiled has two kinds of caching:

1. **Client-side response cache.** The Tiled Python client implements a standard
   web cache, similar in both concept and implementation to a web browser's cache.
3. **Service-side resource cache.** The _response_ caches operate near the outer
   edges of the application, stashing and retrieve HTTP response bytes. The
   _resource_ cache is more deeply integrated into the application: it is
   available for authors of Adapters to use for stashing file handles and
   related system resources that may be useful in expediting future work.

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
