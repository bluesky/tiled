# Simple Tiled Server

The Simple Tiled Server is a utility for easily launching Tiled servers
intended for tutorials and development. It employs only basic security and
should not be used to store anything important. It does not scale to large
number of users. By default, it uses temporary storage.

```python
from tiled.server import SimpleTiledServer
from tiled.client import from_uri

server = SimpleTiledServer()
client = from_uri(server.uri)
```

The server can be stopped by calling `server.close()`. Only one server may
be run at a time, per Python process.

By default, the Simple Tiled Server allocates a temporary directory for
storage, including files, embedded databases, and server logs. The temporary
directory is deleted when the server is stopped.

To use persistent storage, specify a directory. If it does not exist, it will
be created.

```python
server = SimpleTiledServer("my_data/")
```
