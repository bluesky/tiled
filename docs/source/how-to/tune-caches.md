# Tune Caches to Balance Speed and Memory Usage

This section describes a feature of the Tiled *server*. For client-side caching,
see {doc}`../tutorials/caching`.

## Data Cache

The Tiled server Tiled stores chunks of recently-used data in worker memory.
(The ability to externalize the data in a shared cache, like Redis, is planned.)
It can use this to expedite future requests. By default, it will use up to 15%
of RAM (total physical memory) for its data cache. This is meant to leave plenty
of room for data analysis and other memory-hungry software that may be running
on the same machine.

If Tiled is running on a dedicated data server, you may wish to turn this
up as high as 70%. If Tiled is running on a resource-constrained laptop, you may
wish to turn this down or turn it off.

This can be done via configuration:

```yaml
# Given in relative terms...
data_cache:
  bytes_available: 0.40  # 40% of total RAM
```

```yaml
# Given in absolute terms...
data_cache:
  bytes_available: 2_000_000_000 # 2 GB of RAM
```

```yaml
# Disable data cache.
data_cache:
  bytes_available: 0
```

For `tiled serve {pyobject, directory}` it can be configured with a flag:

```
# Given in relative terms...
tiled serve {pyobject, directory} --data-cache=0.40 ...  # 40% of total RAM
```

```
# Given in absolute terms...
tiled serve {pyobject, directory} --data-cache=2_000_000_000 ...  # 2 GB
```

```
tiled serve {pyobject, directory} --data-cache=0 ...  # disabled
```

The server logs the data cache configuration at startup, as in:

```
DATA CACHE: Will use up to 12583450214 bytes (30% of total physical RAM)
```

To log cache hits, misses, and stores, use this configuration setting

```yaml
data_cache:
  bytes_available: ...
  log_level: DEBUG  # case-insensitive
```

or the environment variable

```
TILED_DATA_CACHE_LOG_LEVEL=DEBUG  # case-insensitive
```

The debug interleave with the access logs from uvicorn like this.

```
DATA CACHE: Miss ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
DATA CACHE: Store ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0) (cost=0.003, nbytes=200)
INFO:     127.0.0.1:47744 - "GET /dataframe/full/file0001 HTTP/1.1" 200 OK
DATA CACHE: Hit ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
INFO:     127.0.0.1:47750 - "GET /dataframe/full/file0001 HTTP/1.1" 200 OK
DATA CACHE: Hit ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
INFO:     127.0.0.1:47758 - "GET /dataframe/full/file0001 HTTP/1.1" 200 OK
```
