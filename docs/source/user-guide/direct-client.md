# Use Tiled in Python without an HTTP server

Tiled is designed "service-first", and all clients including the Python one
typically operate using HTTP over TCP. But in some situations it can be
convenient to access a Tree's data more directly in Python. Such as:

* You only care about accessing the data from Python, you don't care about
  enforcing any access controls (the user is trusted), and you don't want to pay
  for transporting the data through the network from service to client.
* You are developing or debugging, and you'd like exceptions from the service
  to be raised directly in the client process.

In these situations, you may run the service and the client in the same process.

## From a (service-side) Tree instance

```py
from tiled.client import Context, from_context
from tiled.examples.generated_minimal import tree
from tiled.server.app import build_app

app = build_app(tree)
context = Context.from_app(app)
client = from_context(context)
```

The communication is all in-process and does not incur network overhead
or the debugging complexity of working with inter-process communication.

## From configuration

The configuration may be given as:

* a dictionary of configuration
* a filepath to a configuration file
* a filepath to a directory of one or more configuration files

From a dict of configuration:

```py
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

config = {
    "trees": [
        {
             "path": "/",
             "tree": "tiled.examples.generated_minimal:tree",
        }
}
app = build_app_from_config(config)
context = Context.from_app(app)
client = from_context(context)
```

From a configuration file:

```py
config = parse_configs("path/to/config.yml")
app = build_app_from_config(config)
context = Context.from_app(app)
client = from_context(context)
```

From a directory of configuration files:

```py
config = parse_configs("path/to/directory/")
app = build_app_from_config(config)
context = Context.from_app(app)
client = from_context(context)
```

## Cleanup

The server's event loop runs on a background thread. To stop that thread, close
the Context:

```py
context.close()
```

Contexts can also be used as context managers:

```py
with Context.from_app(app) as context:
    client = from_context(context)
    ...
```

## Direct access to the service-side object

For advanced debugging, it is sometimes useful to set aside the client
entirely and working directly with the "service-side" Tree object.
The following invocation will get you there.

```py
client.context.http_client.app.state.root_tree
```
