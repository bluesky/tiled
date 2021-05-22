# Control Speed, Memory Usage, and Disk Usage

*There are no solutions, only trade-offs.*

We will build Catalogs with a variety of trade-offs in speed and resource
usage. Any of these may be a good solution depending the specifics of your
situation. They are presented in order of increasing complexity.

## Everything in memory

```python
import numpy
from tiled.readers.array ArrayAdapter
from tiled.catalogs.in_memory import Catalog

# Generate data and store it in memory.
a = numpy.random.random((100, 100))
b = numpy.random.random((100, 100))
catalog = Catalog(
    {
        "a": ArrayAdapter.from_array(a),
        "b": ArrayAdapter.from_array(b),
    }
)
```
* Server startup is **slow** because all data is generated or read up front.
* Data access is **fast** because all the data is ready in memory.
* The machine running the server must have sufficient RAM for all the entries in the Catalog.
  This **is not** a scalable solution for large Catalogs with large data.

## Load on first read

For the next example we'll use a neat dictionary-like object. It is created
by mapping keys to *functions*. The first time a given item is accessed, the
function is called to generate the value, and the result is stashed
internally for next time.

```python
>>> from tiled.utils import OneShotCachedMap
>>> m = OneShotCachedMap({"a": lambda: 1, "b": lambda: 2})
>>> m["a"]  # value is computed on demand, so this could slow
1
>>> m["a"]  # value is returned immediately, remembered from last time
1
>>> dict(m)  # can be converted to an ordinary dict
{"a": 1, "b": 2}
```

Notice that one the object is created, it acts just like a normal Python
mapping. Downstream code does not have to do anything special to work with
it.

It can be integrated with ``Catalog`` directly, just replacing the dictionary
in the first example with a ``OneShotCachedMap``.

```python
import numpy
from tiled.utils import OneShotCachedMap
from tiled.readers.array ArrayAdapter
from tiled.catalogs.in_memory import Catalog

# Use OneShotCachedMap which maps keys to *functions* that are
# run when the data is fist accessed.
catalog = Catalog(
    OneShotCachedMap(
        {
            "a": lambda: ArrayAdapter.from_array(numpy.random.random((100, 100))),
            "b": lambda: ArrayAdapter.from_array(numpy.random.random((100, 100))),
        }
    )
)
```

* Server startup is **fast** because nothing is generated or read up front.
* The first access for each item is **slow** because the data is generated or
  read on demand. Subsequent access is **fast**.
* The machine running the server must have sufficient RAM for all the entries
  in the Catalog. The memory usage will grow monotonically as items are
  accessed and stashed internally by ``OneShotCachedMap``. This **is not** a
  scalable solution for large Catalogs with large data.

## Load on first read and stash for awhile (but not forever)

We'll use another neat dictionary-like object very much like the previous one.
These two objects behave identically...

```python
from tiled.utils import CachingMap, OneShotCachedMap

OneShotCachedMap({"a": lambda: 1, "b": lambda: 2})
CachingMap({"a": lambda: 1, "b": lambda: 2}, cache={})
```

...except that ``CachingMap`` can regenerate values if need be and allows us
to control which values it stashes and for how long. For example, using the
third-party library ``cachetools`` we can keep up to N items, discarding the least
recently used item when the cache is full.

```
pip install cachetools
```

```python
from cachetools import LRUCache

CachingMap({"a": lambda: 1, "b": lambda: 2}, cache=LRUCache(1))
```

As another example, we can keep items for up to some time limit. This
enforces a maximum size as well, so that the cache cannot grow too large if
it is receiving many requests within the specified time window.

```python
from cachetools import LRUCache

# "TTL" stands for "time to live", measured in seconds.
CachingMap({"a": lambda: 1, "b": lambda: 2}, cache=TTLCache(maxsize=100, ttl=10))
```

If we keep a reference to our cache before passing it in, then other parts
of the program can explicitly evict items to force an update.

```python
cache = TTLCache(maxsize=100, ttl=10)
CachingMap({"a": lambda: 1, "b": lambda: 2}, cache=cache)

# Meanwhile...elsewhere in the code....
cache.pop("a")  # Force a refresh of this item next time it is accessed from CachingMap.
```

``CachingMap`` integrates with ``Catalog`` exactly the same as the others.

```python
from cachetools import TTLCache
import numpy
from tiled.utils import CachingMap
from tiled.readers.array ArrayAdapter
from tiled.catalogs.in_memory import Catalog

# Use CachingMap which again maps keys to *functions* that are
# run when the data is fist accessed. The values may be cached
# for a time.
catalog = Catalog(
    CachingMap(
        {
            "a": lambda: ArrayAdapter.from_array(numpy.random.random((100, 100))),
            "b": lambda: ArrayAdapter.from_array(numpy.random.random((100, 100))),
        },
        cache=TTLCache(maxsize=100, ttl=10),
    ),
)
```

* Server startup is **fast** because nothing is generated or read up front.
* The first access for each item is **slow** because the data is generated or
  read on demand. Later access may be **fast or slow** depending on whether
  the item has been evicted from the cache.
* With an appropriately-scaled cache, this **is** scalable for large Catalogs
  with large data.

## Proxy data from a network service and keep an on-disk cache

TO DO: Add fsspec example --- How to put an upper bound on the disk cache?

## Load keys dynamically as well as values

In all the previous examples, the *keys* of the Catalog were held in memory,
in Python, and the *values* were sometimes generated on demand. For Catalog
at the scale of thousands of entries, backed by a database or a web service,
we'll need to generate the keys on demand too. At that point, we escalate to
writing a custom class satisfying the Catalog specification, which fetches
keys and values on demand by making queries specific to the underlying
storage.

TO DO: Simplify Bluesky MongoDB example into MVP and add it here.