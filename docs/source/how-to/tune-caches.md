# Tune Caches to Balance Speed and Memory Usage

## Object Cache

The Tiled server stores objects such as file handles for frequently-opened
files and chunks of frequently-used data in worker memory. (The ability to
externalize the data in a shared cache, like Redis, is planned.) It can use this
to expedite future requests. By default, it will use up to 15% of RAM (total
physical memory) for its object cache. This is meant to leave plenty of room for
data analysis and other memory-hungry software that may be running on the same
machine.

If Tiled is running on a dedicated data server, you may wish to turn this
up as high as 70%. If Tiled is running on a resource-constrained laptop, you may
wish to turn this down or turn it off.

This can be done via configuration:

```yaml
# Given in relative terms...
object_cache:
  available_bytes: 0.40  # 40% of total RAM
```

```yaml
# Given in absolute terms...
object_cache:
  available_bytes: 2_000_000_000 # 2 GB of RAM
```

```yaml
# Disable object cache.
object_cache:
  available_bytes: 0
```

For `tiled serve {pyobject, directory}` it can be configured with a flag:

```
# Given in relative terms...
tiled serve {pyobject, directory} --object-cache=0.40 ...  # 40% of total RAM
```

```
# Given in absolute terms...
tiled serve {pyobject, directory} --object-cache=2_000_000_000 ...  # 2 GB
```

```
tiled serve {pyobject, directory} --object-cache=0 ...  # disabled
```

The server logs the object cache configuration at startup, as in:

```
OBJECT CACHE: Will use up to 12583450214 bytes (30% of total physical RAM)
```

To log cache hits, misses, and stores, use this configuration setting

```yaml
object_cache:
  available_bytes: ...
  log_level: DEBUG  # case-insensitive
```

or the environment variable

```
TILED_OBJECT_CACHE_LOG_LEVEL=DEBUG  # case-insensitive
```

The debug logs interleave with the access logs from uvicorn like this.

```
OBJECT CACHE: Miss ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
OBJECT CACHE: Store ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0) (cost=0.003, nbytes=200)
INFO:     127.0.0.1:47744 - "GET /table/full/file0001 HTTP/1.1" 200 OK
OBJECT CACHE: Hit ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
INFO:     127.0.0.1:47750 - "GET /table/full/file0001 HTTP/1.1" 200 OK
OBJECT CACHE: Hit ('dask', 'read-csv-c15bf1fe8e072d8bf571d9809d3f6bcc', 0)
INFO:     127.0.0.1:47758 - "GET /table/full/file0001 HTTP/1.1" 200 OK
```
