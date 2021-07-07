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
from tiled.examples.generated_minimal import tree as service_side_tree
from tiled.client import from_tree

client = from_tree(service_side_tree)
```

This ``service_side_tree`` is *not* generally meant to be used directly,
so we "connect" to it with a client. But, unlike with ``tiled serve ...``
the communication is all in-process and does not incur network overhead
or the debugging complexity of working with inter-process communication.

## From configuration

The configuration may be given as:

* a dictionary of configuration
* a filepath to a configuration file
* a filepath to a directory of one or more configuration files

From a dict of configuration:

```py
from tiled.client import from_config

config = {
    "trees": [
        {
             "path": "/",
             "tree": "tiled.examples.generated_minimal:tree",
        }
}
client = from_config(config)
```

From a configuration file:

```py
client = from_config("path/to/config.yml")
```

From a directory of configuration files:

```py
client = from_config("path/to/directory/")
```

## Direct access to the service-side object

For advanced debugging, it is sometimes useful to set aside the client
entirely and working directly with the "service-side" Tree object.
To construct one from configuration:

```py

from tiled.config import direct_access

service_side_tree = direct_access(config)
```

where, as in the section above, ``config`` may be a dictionary of configuration
or filepath.

To construct one from a profile name:

```py
from tiled.config import direct_access_from_profile

service_side_tree = direct_access_from_profile("profile_name")
```