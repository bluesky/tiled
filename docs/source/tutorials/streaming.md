# Streaming

```{warning}

Streaming is an experimental feature in Tiled, subject to backward-compatible
changes without notice.

Additionally, it is only currently supported on arrays and tables.

```

## Prerequisite: Redis

Tiled's streaming feature requires Redis, which it uses to cache
recent fragments of data (or pointers to large data) and track
when to notify subscribed clients about updates.

You can run it however you wish. For simple testing, this works:

```
docker run -d --rm --name tiled-test-redis -p 6379:6379 docker.io/redis:7-alpine
```

(For production, we recommend configuring Redis with encryption and a password.)

## Launch Tiled Server with Streaming Cache

For simple testing, this is a suitable command:

```
tiled serve catalog --temp --api-key secret --cache redis://localhost:6379
```

```{warning}
Never specify a guessable API key like `secret` in production; let Tiled
generate a random one. This is just a convenience for testing.
```

````{note}

If using a config file, add this section:

    ```yaml
    streaming_cache:
      uri: redis://localhost:6379
    ```
````

## Write and Stream Data

```py
from tiled.client import from_uri

# Connect
client = from_uri('http://localhost:8000', api_key='secret')

# Create a Subscription
sub = client.subscribe()

# Register a callback, a function that will be called when updates are received.
# For a simple example, we will just use the print function.
sub.add_callback(print)

# Start listening for updates.
sub.start()
```

In a separate process (if you like) create a new array.

```py
from tiled.client import from_uri
import numpy

c = from_uri('http://localhost:8000', api_key='secret')
x = c.write_array(numpy.array([1, 2, 3]), key='x')
```

Back in the process with the subscription, you will see that something has been
printed.

```none
<Subscription / > {'sequence': 1, 'timestamp': '2025-09-03T16:58:59.682183', 'key': 'x', 'structure_family': 'array', 'specs': [], 'metadata': {}, 'data_sources': [{'id': None, 'structure_family': 'array', 'structure': {'data_type': {'endianness': 'little', 'kind': 'i', 'itemsize': 8, 'dt_units': None}, 'chunks': [[3]], 'shape': [3], 'dims': None, 'resizable': False}, 'mimetype': 'application/x-zarr', 'parameters': {}, 'assets': [], 'management': 'writable'}], 'uri': 'http://localhost:8000/api/v1/array/full/'}
```

Our callback, `print`, has been called with the arguments `print(sub, data)`
where `sub` is our Subscription and `data` is a dict with information
about what is new. In this case, it contains a detailed description of the
new array named `x` that was created by the other client.

If we are interested, we can subscribe to updates about `x` and its data.

```py
x_sub = c['x'].subscribe()
```

Suppose that, while we are getting that set up, the other process extends the
array with more data.

```py
x.patch(numpy.array([4, 5, 6]), offset=3, extend=True)
```

We can ensure that subscriber sees the full (recent) picture by specifying that
its subscription should start from as far back as the server has available.
(By default, the server retains an hour of history in Redis for fast streaming
access, but it may shed history earlier if it grows short on available RAM.)

This feature is not designed to provide a comprehensive history, only to allow
clients to catch up if they start late---such as a live data processing job
launched after an experiment is already in progress.

```py
x_sub.start(0)
```

It will receive updates that have already happened:

```none
<Subscription /x > {'sequence': 1, 'timestamp': '2025-09-03T17:37:29.754038', 'content-type': 'application/octet-stream', 'shape': [3], 'offset': None, 'block': None, 'uri': 'http://localhost:8000/api/v1/array/full/x', 'payload': b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00'}
<Subscription /x > {'sequence': 2, 'timestamp': '2025-09-03T17:37:36.678614', 'content-type': 'application/octet-stream', 'shape': [3], 'offset': [3], 'block': None, 'uri': 'http://localhost:8000/api/v1/array/full/x', 'payload': b'\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00'}
```

and, from there, any new updates as well.

Notice that content of the updates includes a `sequence` counter, starting from
the number 1 (with 0 reserved to unambiguously define the "start of the stream"). Subscribers can use this if, for example, they need to recover
from an interruption. They can subscribe from a specific counter index `sub.start(N)`.
As already mentioned above, `x.start(0)` means, "Start from the oldest record retained."

Of course, clients can always fetch _all_ the data via the non-streaming
interface.

The writer can let subscribers know that no more data is expected (for now).
This does not prevent them from writing more later; it just signal that
nothing more is expected. Live data processing jobs may use this as
a prompt to clean up and free up resources.

```py
x.close_stream()
```

This will stop the threads that are listening for updates and set `sub.closed`
to `True`. Alternatively, a subscriber can unsubscribe even if the stream
is still going:

```py
sub.close()
```

## Limitations

This feature is in a very early preview stage.

- Other data structures (table, sparse, awkward) are not yet supported.
- Updates to metadata and deletions of nodes are not yet visible to subscribers.
