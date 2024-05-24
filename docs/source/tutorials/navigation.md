# Navigate with the Python Client

In this tutorial we will navigate a collection of datasets using Tiled's Python
client.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve demo
```

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
```

This holds a nested structure of data. Conceptually, it corresponds well to
a directory of files or hierarchical structure like an HDF5 file or XML file.

Tiled provides a utility for visualizing a nested structure.

```python
>>> from tiled.utils import tree
>>> tree(client)
├── big_image
├── small_image
├── medium_image
├── sparse_image
├── awkward_array
├── tiny_image
├── tiny_cube
├── tiny_hypercube
├── short_table
├── long_table
├── wide_table
├── structured_data
│   ├── pets
│   └── xarray_dataset
```

Each (sub)tree displays the names of a couple of its entries---up to
however many fit on one line.


```python
>>> client
<Container {'big_image', 'small_image', 'medium_image', ...} ~16 entries>
```

Containers act like (nested) mappings in Python. All the (read-only) methods
that work on Python dictionaries work on Containers. We can lookup a specific
value by its key

```python
>>> client['structured_data']
<Container {'pets', 'xarray_dataset'}>
```

list all the keys

```python
>>> list(client)
['big_image',
 'small_image',
 'medium_image',
 'sparse_image',
 'awkward_array',
 'tiny_image',
 'tiny_cube',
 'tiny_hypercube',
 'short_table',
 'long_table',
 'wide_table',
 'structured_data',
 'flat_array',
 'low_entropy',
 'high_entropy',
 'dynamic']
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

Containers also support efficient list-like access. This is useful for quickly
looking at a couple or efficiently grabbing batches of items, especially if you
need to start from the middle.

```python
>>> client.keys().first()  # Acces the first key.
'big_image'

>>> client.keys().head()  # Access the first several keys.
['big_image',
 'small_image',
 'medium_image',
 'sparse_image',
 'awkward_array']

>>> client.keys().head(3)  # Access the first N keys.
['big_image',
 'small_image',
 'medium_image']

>>> client.keys()[1:3]  # Access just the keys for entries 1:3.
['small_image', 'medium_image']
```

All the same methods work for values

```python
>>> client.values()[1:3]  # Access the values (which may be more expensive).
[<ArrayClient shape=(300, 300) chunks=((300,), (300,)) dtype=float64>, <ArrayClient shape=(1000, 1000) chunks=((1000,), (1000,)) dtype=float64>]
```

and `(key, value)` pairs ("items").

```python
>>> client.items()[1:3]  # Access (key, value) pairs.
[('small_image', <ArrayClient shape=(300, 300) chunks=((300,), (300,)) dtype=float64>),
('medium_image', <ArrayClient shape=(1000, 1000) chunks=((1000,), (1000,)) dtype=float64>)]
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

See a later tutorial for how to search Containers with queries.
