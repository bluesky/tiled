# Working with `bytes` nodes

The `bytes` structure family lets Tiled catalog opaque binary files -- PDFs,
firmware blobs, proprietary binary formats, raw detector dumps -- without
asking Tiled to interpret them. See
[Bytes](bytes-structure-family) for an explanation of the family
itself; this how-to focuses on the practical operations: registering bytes
content, downloading it with the Python client and from the HTTP API, and
handling multi-asset nodes.

## Auto-registration with `tiled register`

`tiled register` infers a MIME type per file from its extension and looks up
the adapter in the registration map. Out of the box, the extensions that are
classified as `application/octet-stream` (`.bin`, `.so`, `.a`, `.o`, `.dump`,
`.pkg`, …) are registered as `bytes` nodes.

Files with extensions that the server doesn't recognise are skipped. To opt
into a true catch-all, supply a `mimetype_detection_hook` that falls back to
`application/octet-stream`:

```python
def detect(path, mimetype):
    return mimetype or "application/octet-stream"
```

The `bytes` family is also the fallback adapter for any unknown structure
family. Mapping a custom MIME type (e.g. `application/pdf`) to `BytesAdapter`
in `adapters_by_mimetype` is enough to register matching files as bytes nodes.

## Downloading with the Python client

The high-level entry point is `BaseClient.raw_export(...)`, the same API used
for downloading the raw files backing arrays, tables, and other structures.
For `bytes` nodes it is the *primary* way to retrieve content, because the
family has no `read()` method.

### To a directory on disk

```python
paths = c["my_blob"].raw_export("downloads/")
```

Downloads happen in parallel. A single-asset node produces one file in the
destination directory, named from the server-supplied filename. A multi-asset
node produces `<destination>/<asset_id>/<filename>` per asset.

### Into memory

`raw_export` also accepts any
[`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)
(for example a plain `dict`) as the destination. Each asset is streamed into
an [`io.BytesIO`](https://docs.python.org/3/library/io.html#io.BytesIO)
buffer; no filesystem I/O is performed:

```python
buffers = {}
keys = c["my_blob"].raw_export(buffers)
payload = buffers[keys[0]].read()   # single-asset node
```

For a multi-asset node, the keys are `<asset_id>/<filename>` and the assets
arrive in arbitrary (parallel-download) order. Each underlying asset carries
a server-side ordering hint (`Asset.num`); reassemble the payload by ordering
the keys by `num`:

```python
ds = c["my_blob"].include_data_sources().data_sources()[0]
num_by_id = {a.id: a.num for a in ds.assets}
ordered = sorted(keys, key=lambda k: num_by_id[int(k.split("/")[0])])
payload = b"".join(buffers[k].read() for k in ordered)
```

## Downloading from the HTTP API

`bytes` nodes do not have a dedicated content endpoint. Each underlying asset
is downloaded individually through the generic
`/api/v1/asset/bytes/{path}?id=N` endpoint, where `N` is the asset's `id`
discovered from the node's metadata (`data_sources[0].assets[i].id`). For
multi-asset nodes, issue one request per asset and concatenate the responses
in `num` order.

The endpoint honors the HTTP `Range:` header and returns `206 Partial
Content` for ranged requests, so large assets are usable with `curl -r`,
`aria2c -x16`, browsers, and other resumable-download tools without any
Tiled-specific client code.

## Disabling raw asset downloads

The `/asset/bytes` endpoint is gated by `settings.expose_raw_assets`, which
defaults to `True`. Administrators can disable raw asset downloads by setting
`expose_raw_assets: False` in the server configuration. With the endpoint
disabled, `bytes` nodes can still be registered and listed but their content
cannot be downloaded.

## Limitations

Currently only `file://` assets are downloadable through the `/asset/bytes`
endpoint. Object-store (`s3://`, `az://`, `gs://`) bytes assets are recorded
in the catalog (and their `Asset.size` can be populated via
`tiled.storage.size_from_uri`), but their content cannot yet be served via
the API; this restriction will be lifted in a future release.
