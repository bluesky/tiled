---
orphan: true

---

# Client Profiles Reference

This is a comprehensive reference. See also {doc}`../how-to/profiles` for a
practical guide with examples.

A profiles YAML file must contain a mapping with one or more keys.
The keys may be any string. The value of each entry is described below.

The content below is automatically generated from a schema that is used
to validate profiles when they are read.
(schema_uri)=
## uri

URI of a Tiled server, such as

    http://localhost:8000


(schema_username)=
## username

For authenticated Catalogs. Optional unless the Catalog requires authentication.


(schema_offline)=
## offline

False by default. When true, rely solely on cache. Do not attempt to connect to server.


(schema_direct)=
## direct

In-line service configuration. See Service Configuration reference.

(schema_structure_clients)=
## structure_clients

Client to read structure into.
Default ("numpy") uses numpy arrays, pandas DataFrames, and xarrays backed
by numpy arrays.

```yaml
structure_clients: "numpy"
```

The "dask" option uses the dask-based analogues of
these.

```yaml
structure_clients: "dask"
```

To use custom clients, map each structure family you want to support
to an import path:

```yaml
structure_clients:
  array: "package.module:CustomArrayClient"
  dataframe: "package.module:CustomDataFrameClient"
```


(schema_cache)=
## cache

(schema_cache.memory)=
### cache.memory

(schema_cache.memory.available_bytes)=
#### cache.memory.available_bytes

Maximum memory (in bytes) that the cache may consume.

For readability it is recommended to use `_` for thousands separators.
Example:

```
available_bytes: 2_000_000_000  # 2GB
```


(schema_cache.memory.error_if_full)=
#### cache.memory.error_if_full

By default, the cache starts evicting the least-used items when
it fills up. This is generally fine when working with a
connection to the server. But if the goal is to cache for
*offline* use, it is better to be notified by and error that the
cache is full. Then the user can respond by increasing
available_bytes, using a different storage volume for the cache,
or choosing to a different (smaller) set of entries to download.


(schema_cache.disk)=
### cache.disk

(schema_cache.disk.path)=
#### cache.disk.path

A directory will be created at this path if it does not yet exist.
It is safe to reuse an existing cache directory and to share a cache
directory between multiple processes.
available_bytes:


(schema_cache.disk.available_bytes)=
#### cache.disk.available_bytes

Maximum storage space (in bytes) that the cache may consume.

For readability it is recommended to use `_` for thousands separators.
Example:

```
available_bytes: 2_000_000_000  # 2GB
```


(schema_cache.disk.error_if_full)=
#### cache.disk.error_if_full

By default, the cache starts evicting the least-used items when
it fills up. This is generally fine when working with a
connection to the server. But if the goal is to cache for
*offline* use, it is better to be notified by and error that the
cache is full. Then the user can respond by increasing
available_bytes, using a different storage volume for the cache,
or choosing to a different (smaller) set of entries to download.


(schema_cache.disk.cull_on_startup)=
#### cache.disk.cull_on_startup

If reusing an existing cache directory which is already larger than the
available_bytes, an error is raised. Set this to True to delete
items from the cache until it fits in available_bytes. False by default.


(schema_token_cache)=
## token_cache

Filepath to directory of access tokens.
Default location is usually suitable.
