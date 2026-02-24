# Simple (Embedded) Server

The Simple Tiled Server is a utility for easily launching Tiled servers
intended for tutorials and development. It employs only basic security and does
not scale to large numbers of users.

By default, it uses temporary storage, but it can be configured to use
persistent storage.

## Quickstart

This convenience function {py:func}`tiled.client.simple` launches a Tiled
server in the background (on a thread) and returns a client connected to it.

```python
from tiled.client import simple

# Default: temporary storage!
c = simple()

# Or use persistent storage
c = simple('path/to/some/directory')
```

When the server starts, a URL is printed to the console. Each launch generates a
unique secret `api_key`. You can paste this URL into a browser to open Tiled's
web interface.

## Readable storage

Detectors or analysis programs often write files directly to disk. Tiled can
make those files accessible without any re-uploading or reformatting.

For security reasons, the server administrator must designate which directories
data can be registered from, like so.

```python
c = simple(readable_storage=['path/to/some/directory/', 'another_directory/'])
```

## Manual server management

Under the hood, {py:func}`tiled.client.simple` uses
{py:class}`tiled.server.SimpleTiledServer`.

```python
from tiled.server import SimpleTiledServer
from tiled.client import from_uri

server = SimpleTiledServer()
client = from_uri(server.uri)
```

The `readable_storage` option works the same way.

```python
server = SimpleTiledServer(
    readable_storage=['path/to/some/directory/', 'another_directory/']
)
```

The server can be stopped by calling `server.close()`, or used as a context
manager, which stops it automatically on exit:

```python
with SimpleTiledServer() as server:
    client = from_uri(server.uri)
    ...
```

By default, the Simple Tiled Server allocates a temporary directory for
storage, including files, embedded databases, and server logs. Its location is
given at `server.directory`. The temporary directory is removed when the server
is stopped.

To use persistent storage, specify a directory. If it does not exist, it will
be created. It will _not_ be removed when the server is stopped.

```python
server = SimpleTiledServer("my_data/")
```

You can specify the port. By default, an available high port is randomly
chosen.

```python
server = SimpleTiledServer(port=8000)
```

You can also manually set the API key. This is sometimes handy for
development, if you find yourself continually copy/pasting the server URL after
relaunching the server. A hard-coded API key should _not_ be used in production.

```python
server = SimpleTiledServer(api_key="secret", port=8000)
```

These options are also available in the "quickstart"
{py:func}`tiled.client.simple` function.

````{warning}
If the server is garbage collected, it will be stopped. The example below will
not work because there is no reference to `server` kept in scope.

```python
# Do not do this.

def f():
    server = SimpleTiledServer()
    client = from_uri(server.uri)
    return client

c = f()
# The server is started but then garbage collected and stopped.
```

The client will show errors like:

```
ConnectError('[Errno 111] Connection refused')
```

The {py:func}`tiled.client.simple` utility solves this problem by keeping
references to the servers that it starts in a list,
`tiled.client.constructors.SERVERS`. They are shut down when the Python
interpreter exits.

````
