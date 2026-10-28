# Asynchronous Client (Experimental)

Tiled includes an asynchronous counterpart to its Python client, for use
inside `asyncio` applications: GUIs and TUIs that must not block the event
loop, services that talk to Tiled while serving their own requests, and
workflows that benefit from issuing many requests concurrently.

```{warning}
The async client is **experimental**. Its API may change without notice,
and its first version deliberately supports a subset of the synchronous
client's features (see {ref}`async-limitations` below).
```

## Connecting

Construction performs no I/O; connecting is explicit and awaited. Close the
connection by closing the client's context, most easily with `async with`:

```python
from tiled.client.aio import from_uri

client = await from_uri("https://tiled.example.com", api_key="...")
async with client.context:
    ...
```

Only API-key authentication is supported in this first version.

## Reading

The family clients mirror their synchronous counterparts, with coroutines in
place of blocking methods, returning in-memory structures (numpy arrays,
pandas DataFrames, and so on; there is no dask integration):

```python
node = await client.lookup(
    "experiment", "scan_042"
)  # instead of client["experiment"]["scan_042"]
array = await node.read()
table = await (await client.lookup("tables", "results")).read(["a", "b"])
```

A read that is too large for one request (a large array, a multi-partition
table) is split into multiple requests and fetched *concurrently*, bounded
by the connection limit of the client's context. Independent operations can
be fanned out the same way:

```python
import asyncio

nodes = await asyncio.gather(*(client.lookup(key) for key in keys))
arrays = await asyncio.gather(*(node.read() for node in nodes))
```

## Containers

`AsyncContainer` is *not* a `Mapping`, because the `[]`, `len()`, and iteration
protocols cannot await. Instead, use explicit coroutines and async iterators:

| Synchronous | Asynchronous |
|---|---|
| `container["a", "b"]` | `await container.lookup("a", "b")` |
| `container.get("a")` | `await container.get("a")` |
| `len(container)` | `await container.len()` |
| `for key in container:` | `async for key in container:` |
| `container.keys()` / `.values()` / `.items()` | `async for ... in container.keys()` / `.values()` / `.items()` |

Searching and sorting are identical to the synchronous client (they build a
new client without any I/O):

```python
from tiled.queries import Key

results = container.search(Key("color") == "red").sort(("color", 1))
async for key, node in results.items():
    ...
deep = container.search_deep(Key("sample_id") == "abc123")
async for path, node in deep.items():
    ...
```

## Writing

The `write_*` methods mirror the synchronous client:

```python
node = await container.create_container("processed", metadata={"source": "beamline"})
array_client = await node.write_array(array, key="image", metadata={"exposure": 0.1})
table_client = await node.write_table(dataframe, key="results")
```

If an array is too large for one request, it is uploaded in chunks, concurrently.
Metadata updates (`update_metadata`, `patch_metadata`, `replace_metadata`)
and deletion (`delete`, `delete_contents`) are coroutines with the same
signatures as their synchronous counterparts.

(async-limitations)=
## Known limitations of the async client

The following synchronous-client features are deliberately absent for now, but may be added in the future:

- authentication other than API keys (no interactive login, no OAuth flows);
- the HTTP response cache;
- dask integration (reads return in-memory structures) and the public
  block-wise access methods that exist to support dask;
- `export` / `raw_export`, and progress bars;
- metadata revisions;
- websocket subscriptions (streaming);
- pickling (an async client is bound to its event loop; reconnect in the
  target process instead);
- `register` (file-registration tooling).

The test `tests/client/test_aio.py::test_surface_parity` is the
authoritative, per-family ledger of these divergences.

## Registering a custom async client

Just as packages can register custom synchronous clients under the
`tiled.structure_client` entry-point group (see
{doc}`custom-clients`), async clients may be registered under
`tiled.structure_client_async`, keyed by spec name or structure family.
