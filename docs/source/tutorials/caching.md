# Keeping a Local Copy

There are several modes of *caching* supported by Tiled. They cover different
situations. Each one is addressed here with a hypthoetical scenario.

## Make repeated access fast within one working session / process

*Solution: Stash results in memory (RAM).*

```python
from tiled.client.catalog import Catalog
from tiled.client.cache import Cache

catalog = Catalog.from_uri("http://...", cache=Cache.in_memory(2e9))
```

where we have to specify the maximum RAM we are willing to dedicate to the cache,
here set to ``2e9``, 2 GB.

Most things that we do with a Catalog or dataset make an HTTP request to the
server and receive a response. For example...

```python
>>> catalog  # Downloads the first couple entry names to display them.
Catalog({"some_dataframe", "another_dataframe", ... (N entries)})
>>> catalog.metadata  # Downloads the metadata for this Catalog.
{"color": "red"}
>>> catalog["some_dataframe"]  # Downloads the column names to display.
... <snipped for brevity>
>>> catalog["some_dataframe"].metadata  # Downloads the metadata for this DataFrame.
{"flavor": "salty"}
>>> catalog["some_dataframe"].read().compute()  # Downloads the data, in several partitions.
... <snipped for brevity>
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

## Make repeated access fast across one working session / process

*Solution: Stash results on disk.*

```python
:emphasize-lines 4

from tiled.client.catalog import Catalog
from tiled.client.cache import Cache

catalog = Catalog.from_uri("http://...", cache=Cache.on_disk("my_cache_directory"))
```

This works exactly the same as before, but now the data is stored in files on disk.
The data can be shared across processes and reused betweening working sessions.

Some things to know:

* You can place an upper limit on how much disk space this is allowed to use.
  By default it will use all the space available on the disk minus 1 GB.
* The directory will be created if it doesn't yet exist.
* It is safe to reuse the same directory for multiple different Catalogs.
  The files will not collide.
* It is safe to share a directory across concurrent processes. The on-disk
  cache uses file-based locking to stay consistent.
* The naming and format of the files is internal to Tiled. It is not intended to be
  accessed by other programs or directly touched by the user. For export files for
  use by other programs see a later section.

## Work offline in "airplane mode" (no network connection)

*Solution: Proactively download a Catalog into Cache that can be used offline.*

First, when connected to the Internet, connect a Catalog and download it.

```python
from tiled.client.cache import download
from tiled.client.catalog import Catalog

catalog = Catalog.from_uri("http://...")
download(catalog, "my_cache_directory")
```

This will downloaded everything needed for basic usage. Note it cannot
support open-ended *search* functionality because the space of possible
queries is too large, but specific search results can be cached by
just running the search while connected:

```python
catalog.search(...)
```

TO DO: Demonstrate downloading only a *portion* of a Catalog.

```{note}

Alternatively, a basic download can be performed from the commandline via the
tiled CLI.

```

$ tiled download "http://..." my_cache_direcotry

In normal *online* operation, Tiled will still "phone home" to the server
just to check that its cached copy is still the most recent version of
the meadata and data. By setting ``offline=True`` we tell it not to
attempt to connect and to rely entirely on its local cache.

```python
:emphasize-lines 4

from tiled.client.catalog import Catalog
from tiled.client.cache import Cache

catalog = Catalog.from_uri("http://...", cache=Cache.on_disk("my_cache_directory"), offline=True)
```

If you attempt to access something that was not downloaded a
``NotAvailableOffline`` error will be raised.

## Export files for use by external programs ("deliberate export")

First, consider alternatives:

If you data analysis is taking place in Python, then you may have
no need to export files. Your code will be faster and simpler if you
work numpy, pandas, and/or xarray structures directly.

If your data analysis is in another language, can it access the data
from the Tiled server directly over HTTP? Tiled supports efficient
formats (e.g. numpy C buffers, Apache Arrow DataFrames) and universal
interchange formats (e.g. CSV, JSON) and perhaps one of those will be the
fastest way to get data into your program.

But sometimes, to integrate with existing software, especially closed-source
software, we need to generate ordinary files on disk. Tiled provides a
utility in the Python client to make that easier.

The Tiled server and client can translate arrays, dataframes, xarrays, and
heirarchical structures to various formats. It's a very basic export functionality,
intentionally not very configurable.

```python
from tiled.utils import export

export(catalog["some_dataframe"], "table.csv")
```

To do more sophisticated export, use standard Python tools, as in:

```python
catalog["some_dataframe"]["A"].to_csv("table.csv")
```

```{note}

The set off formats supported is extensible and depends on the software
environment (i.e. what's installed).
```