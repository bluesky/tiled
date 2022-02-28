# Navigate with the Python Client

In this tutorial we will navigate a collection of datasets using Tiled's Python
client.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:tree
```

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000/api")
```

This holds a nested structure of data. Conceptually, it corresponds well to
a directory of files or hierarchical structure like an HDF5 file or XML file.

Tiled provides a utility for visualizing a nested structure.

```python
>>> from tiled.utils import tree
>>> tree(client)
├── big_image
├── small_image
├── tiny_image
├── tiny_cube
├── tiny_hypercube
├── low_entropy
├── high_entropy
├── short_table
├── long_table
├── labeled_data
│   └── image_with_dims
└── structured_data
    ├── image_with_coords
    └── xarray_dataset
```

Each (sub)tree displays the names of a couple of its entries---up to
however many fit on one line.


```python
>>> client
<Node {'big_image', 'small_image', 'tiny_image', 'tiny_cube', ...} ~11 entries>
```

Nodes act like (nested) mappings in Python. All the (read-only) methods
that work on Python dictionaries work on Nodes. We can lookup a specific
value by its key

```python
>>> client['structured_data']
<Node {'image_with_coords', 'xarray_dataset'}>
```

list all the keys

```python
>>> list(client)
['big_image',
 'small_image',
 'tiny_image',
 'tiny_cube',
 'tiny_hypercube',
 'low_entropy',
 'high_entropy',
 'short_table',
 'long_table',
 'labeled_data',
 'structured_data']
```

and loop over keys, values, or ``(key, value)`` pairs.

```python
for key in client:
    ...

# This is equivalent:
for key in client.keys():
    ...

for value in client.values():
    ...

for key, value in client.items():
    ...
```

Nodes also support list-like access, via special attributes. This is useful
for efficiently grabbing batches of items, especially if you need to start
from the middle.

```python
>>> client.keys_indexer[1:3]  # Access just the keys for entries 1:3.
['small_image', 'tiny_image']

>>> client.values_indexer[1:3]  # Access the values (which may be more expensive).
[<ArrayClient>, <ArrayClient>]

>>> client.items_indexer[1:3]  # Access (key, value) pairs.
[('small_image', <ArrayClient>),
[('tiny_image', <ArrayClient>),
```

Each item has ``metadata``, which is a simple dict.
The content of this dict has no special meaning to Tiled; it's the user's
space to use or not.

```python
>>> client.metadata  # happens to be empty
DictView({})

>>> client['short_table'].metadata  # happens to have some stuff
DictView({'animal': 'dog', 'color': 'red'})
```

See a later tutorial for how to search Nodes with queries.
