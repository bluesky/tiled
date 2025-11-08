# Streaming

```{warning}

Streaming is an experimental feature in Tiled, subject to backward-compatible
changes without notice.

Additionally, it is only currently supported on containers, arrays (both
uploaded and external registered data), and tables (uploaded data only).
Support for external registered tabular data and other structures (sparse,
awkward) is planned but not yet implemented.

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

(write-stream-data)=
## Write and Stream Data

```py
from tiled.client import from_uri

# Connect
client = from_uri('http://localhost:8000', api_key='secret')

# Create a Subscription
sub = client.subscribe()

# Register a callback, a function that will be called when updates are received.
def on_child_created(update):
    print("NEW:", update.child())

sub.child_created.add_callback(on_child_created)

# Start listening for updates.
sub.start_in_thread()
```

In a separate process (if you like) create a new array.

```py
from tiled.client import from_uri
import numpy

client = from_uri('http://localhost:8000', api_key='secret')
x = client.write_array(numpy.array([1, 2, 3]), key='x')
```

Back in the process with the subscription, you will see that something has been
printed.

```none
NEW: <ArrayClient shape=(3,) chunks=((3,)) dtype=int64>
```

If we are interested, we can subscribe to updates about `x` and its data.

```py
x_sub = c['x'].new_data.subscribe()

def on_new_data(update):
    print("offset:", update.offset)
    print(update.data())

x_sub.new_data.add_callback(on_new_data)
```

Suppose that, while we are getting that set up, the other process extends the
array with more data in `x`:

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
x_sub.start_in_thread(0)
```

It will receive updates that have already happened:

```none
offset: (0,)
[1, 2, 3]
offset: (3,)
[4, 5, 6]
```

and, from there, any new updates as well.

The `update` includes a `sequence` counter, starting from the number 1.
Subscribers can use this if, for example, they need to recover from an
interruption. They can subscribe from a specific counter index
`sub.start_in_thread(N)`. As already mentioned above, `x.start_in_thread(0)`
means, "Start from the oldest record retained."

Of course, clients can always fetch _all_ of the data via the non-streaming
interface.

## Disconnecting or Closing

A subscriber can disconnect from a stream at any point, like so:

```py
sub.disconnect()
```

Producers (writers) can indicate that no more data is expected (for now).

```py
x.close_stream()
```

This will cause the server to disconnect any active subscribers, once
they are caught up to the last item in the stream. (On a protocol level,
this sends the WebSocket code `1000 ConnectionClosedOK`. Clients can tell
that they were disconnected due to stream completion, not an error.) In the
Python client, this will stop the threads that are listening for updates and
it will set `sub.closed` to `True`.

Writing can still resume writing later---or even immediately. Closing a stream
signals that consumers should not _expect_ any more data soon, and forces them
to affirmatively re-subscribe if they want to watch for any. Live data
processing jobs may use this as a prompt to clean up and free up resources.

## Sequence Number Guarantees

While the sequence number is guaranteed to increment by 1 during an active
stream, clients should expect that it _may_ reset back to 1 after a stream has
been idle. This happens quickly (1 hour, by default) if a stream is explicitly
closed or slowly (30 days, by default) if a stream is left un-closed but
dormant. These intervals are configurable via the settings `data_ttl` and
`seq_ttl` respectively under `streaming_cach` configuration.  (In the `tiled
serve` CLI they are `--cache-data-ttl` and `--cache-seq-ttl`.)

## Performance Optimization: Non-persistent Updates

To rapidly stream large arrays of transitory data, it might be unnecessary
or even undesirable to persist the intermediate arrays to storage.
In this case, pass the parameter `persist=False` to `ArrayClient.write()`
or `ArrayClient.patch()`.

This could be useful for serving reconstructed images to an on-screen display
while an iterative algorithm is refining that reconstruction. Only the final
image needs to be persisted. The intermediate images can be discarded as each
new image is received.

To try this yourself, modify the code in [](#write-stream-data) to include the
`persist=False` when sending updates.

```py
# Intial state: x == [1, 2, 3]

# PUT (write) new values
x.write(numpy.array([4, 5, 6]), persist=False)
  # x == [1, 2, 3], update.data() == [4, 5, 6]
x.write(numpy.array([7, 8, 9]), persist=False)
  # x == [1, 2, 3], update.data() == [7, 8, 9]

# Persist the final array
x.write(numpy.array([11, 12, 13]))  # x == [11, 12, 13], persist == True
```

```py
# Intial state: x == [1, 2, 3]

# PATCH (patch) new values
x.patch(numpy.array([11], offset=(0,)), persist=False)
  # x == [1, 2, 3], update.data() == [11], update.offset() == (0,)
x.patch(numpy.array([12], offset=(1,)), persist=False)
  # x == [1, 2, 3], update.data() == [12], update.offset() == (1,)
x.patch(numpy.array([13], offset=(2,)), persist=False)
  # x == [1, 2, 3], update.data() == [13], update.offset() == (2,)

# Persist the final array
x.write(numpy.array([11, 12, 13]))  # x == [11, 12, 13], persist == True
```

The array could initially be left empty to further reduce the writes to disk.

```py
client = from_uri('http://localhost:8000', api_key='secret')
x = client.new(
    structure_family=StructureFamily.array,
    data_sources=[tiff_data_source],  # DataSource details omitted for brevity
    data_sources=[
        # In-memory DataSource details omitted for brevity
        DataSource(structure=structure, structure_family=StructureFamily.array),
        # Or use an external data source, such as a TIFF file
        tiff_data_source,  # DataSource details omitted for brevity
    ],
    key='x',
)
# Intial state: x == [?, ?, ?]

# PUT (write) new values
x.write(numpy.array([4, 5, 6]), persist=False)
  # x == [?, ?, ?], update.data() == [4, 5, 6]
x.write(numpy.array([7, 8, 9]), persist=False)
  # x == [?, ?, ?], update.data() == [7, 8, 9]

# Persist the final array
x.write(numpy.array([11, 12, 13]))  # x == [11, 12, 13], persist == True
```

## Limitations

This feature is in a very early preview stage.

- External registered tabular data is not yet supported.
- Other data structures (sparse, awkward) are not yet supported.
- Arrays can be patched with either `extend=True` or `persist=False`,
  but not both.
- Deletions of nodes are not yet visible to subscribers.
