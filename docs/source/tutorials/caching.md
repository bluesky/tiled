# Keep a Local Copy

Tiled can use a local *cache* to make efficient use of network bandwidth and the
user's time. It avoids downloading something twice if it hasn't changed.
This also enables an offline "airplane mode".

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:demo
```

## Make repeated access fast within one working session / process

*Solution: Stash results in memory (RAM).*

```python
from tiled.client import from_uri
from tiled.client.cache import Cache

client = from_uri("http://localhost:8000", cache=Cache.in_memory(2e9))
```

where we have to specify the maximum RAM we are willing to dedicate to the cache,
here set to ``2e9``, 2 GB.

Most things that we do with a Tree or dataset make an HTTP request to the
server and receive a response. For example...

```python
>>> client = from_uri("http://localhost:8000", cache=Cache.in_memory(2e9))  # fetches some metadata

>>> client
<Tree {'arrays', 'dataframes', 'xarrays', 'nested', ...} ~5 entries>   # fetches the first couple entry names

>>> client.metadata  # fetches metadata (in this case empty)
DictView({})

>>> client['dataframes']['df']  # fetches column names
<DataFrameClient ['A', 'B', 'C']>

>>> client['dataframes']['df'].metadata  # fetches metadata (in this case empty)
DictView({})

>>> client['dataframes']['df'].read().compute()  # fetches data, in partitions
              A         B         C
index                              
0      0.748885  0.769644  0.296070
1      0.071319  0.364743  0.718473
2      0.322665  0.897854  0.558606
3      0.328785  0.810159  0.073775
4      0.158253  0.822505  0.637224
...         ...       ...       ...
95     0.913758  0.488304  0.615120
96     0.969652  0.287850  0.288405
97     0.769774  0.941785  0.353047
98     0.350033  0.052412  0.969244
99     0.356245  0.683540  0.166682

[100 rows x 3 columns]
```

If run any of the code above, a second time, we'll find that it's faster.

How does it work? Each HTTP request and response is captured inside
``Cache``. If the same operation is performed again later, we send a request
to the server to check whether the content has changed since our last
request, and it only sends a fresh copy if it has. Otherwise, we can use the
copy in our ``Cache`` to save bandwidth and time. All of this happens
automatically.

If the size limit is reached, ``Cache`` evicts entries to make room for new
ones. It decides what to evict based on a "score" that takes into account how
long it would take to re-download and how often it's been used recently.

Because the ``Cache.in_memory(...)`` stores data in RAM it only applies to
specific Python process. Once Python exits (or a Jupyter kernel is restarted)
or data is lost and will need to be cached anew. To persist it for longer,
see the next section.

## Make repeated access fast across working sessions / processes

*Solution: Stash results on disk.*

```{code-block} python
:emphasize-lines: 4

from tiled.client import from_uri
from tiled.client.cache import Cache

client = from_uri("http://localhost:8000", cache=Cache.on_disk("my_cache_directory"))
```

This works exactly the same as before, but now the data is stored in files on disk.
The data can be shared across processes and reused between working sessions.

Some things to know:

* You can place an upper limit on how much disk space this is allowed to use.
  By default it will use all the space available on the disk minus 1 GB.
* The directory will be created if it doesn't yet exist.
* It is safe to reuse the same directory for multiple different Trees.
  The files will not collide.
* It is safe to share a directory across concurrent processes. The on-disk
  cache uses file-based locking to stay consistent.
* The naming and format of the files is internal to Tiled. It is not intended to be
  accessed by other programs or directly touched by the user. For export files for
  use by other programs see a later section.

## Work offline in "airplane mode" (no network connection)

*Solution: Proactively download a Tree into Cache that can be used offline.*

First, when connected to the Internet, connect a Tree and download it.

```python
from tiled.client.cache import download
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
download(client, "my_cache_directory")
```

This will downloaded everything needed for basic usage. Note it cannot
support open-ended *search* functionality because the space of possible
queries is too large, but specific search results can be cached by
just running the search while connected:

```python
client.search(...)
```

TO DO: Demonstrate downloading only a *portion* of a Tree.

```{note}

Alternatively, a basic download can be performed from the command line via the
tiled CLI.

    $ tiled download "http://localhost:8000" my_cache_directory

```

In normal *online* operation, Tiled will still "phone home" to the server
just to check that its cached copy is still the most recent version of
the meadata and data. By setting ``offline=True`` we tell it not to
attempt to connect and to rely entirely on its local cache.

```{code-block} python
:emphasize-lines: 4

from tiled.client import from_uri
from tiled.client.cache import Cache

client = from_uri("http://localhost:8000", cache=Cache.on_disk("my_cache_directory"), offline=True)
```

If you attempt to access something that was not downloaded a
``NotAvailableOffline`` error will be raised.

## Comparison to "deliberate export"

When caching, Tiled takes control of the data. The format, layout, and naming of
the files in the cache should be treated as internal to Tiled. It is up to Tiled
to manage them, and they should not be modified or directly accessed by user code.

To save files for use by user code or other software, use {doc}`export`.
