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
├── scalars
│   ├── pi
│   ├── e_arr
│   ├── fsc
│   └── fortytwo
├── nested
│   ├── images
│   │   ├── tiny_image
│   │   ├── small_image
│   │   ├── medium_image
│   │   └── big_image
│   ├── cubes
│   │   ├── tiny_cube
│   │   └── tiny_hypercube
│   ├── complex
│   ├── sparse_image
│   └── awkward_array
├── tables
│   ├── short_table
│   ├── long_table
<Output truncated at 20 lines. Adjust tree's max_lines parameter to see more.>
```

Each (sub)tree displays the names of a couple of its entries---up to
however many fit on one line.


```python
>>> client
<Container {'scalars', 'nested', 'tables', 'structured_data', ...} ~8 entries>
```

Containers act like (nested) mappings in Python. All the (read-only) methods
that work on Python dictionaries work on Containers. We can

* lookup a specific value by its key

```python
>>> client['structured_data']
<Container {'pets', 'xarray_dataset'}>
```

* easily access nested hierarchies

```python
>>> client['nested']['images']['tiny_image']
<ArrayClient shape=(50, 50) chunks=((50,), (50,)) dtype=float64>
```

* or using a simplified syntax

```python
>>> client['nested', 'images', 'tiny_image']
<ArrayClient shape=(50, 50) chunks=((50,), (50,)) dtype=float64>
```

* or, equivalently

```python
>>> client['nested/images/tiny_image']
<ArrayClient shape=(50, 50) chunks=((50,), (50,)) dtype=float64>
```

* list all the keys

```python
>>> list(client)
['scalars',
 'nested',
 'tables',
 'structured_data',
 'flat_array',
 'low_entropy',
 'high_entropy',
 'dynamic']
```

* and loop over keys, values, or ``(key, value)`` pairs

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
>>> client.keys().first()  # Access the first key.
'scalars'

>>> client.keys().head()  # Access the first several keys.
['scalars',
'nested',
'tables',
'structured_data',
'flat_array']

>>> client.keys().head(3)  # Access the first N keys.
['scalars',
'nested',
'tables']

>>> client.keys()[1:3]  # Access just the keys for entries 1:3.
['nested', 'tables']
```

All the same methods work for values, which return string representations of the
container contents:

```python
>>> client.values()[1:3]  # Access the values (which may be more expensive).
[<Container {'images', 'cubes', 'complex', 'sparse_image', ...} ~5 entries>,
 <Container {'short_table', 'long_table', 'wide_table'}>]
```

or

```python
>>> client['nested/images'].values()[:2]  # Access the values of a nested container
[<ArrayClient shape=(50, 50) chunks=((50,), (50,)) dtype=float64>,
 <ArrayClient shape=(300, 300) chunks=((300,), (300,)) dtype=float64>]
```

and `(key, value)` pairs ("items").

```python
>>> client['nested/images'].items()[:2]  # Access (key, value) pairs.
[('tiny_image',
  <ArrayClient shape=(50, 50) chunks=((50,), (50,)) dtype=float64>),
 ('small_image',
  <ArrayClient shape=(300, 300) chunks=((300,), (300,)) dtype=float64>)]
```

Each item has ``metadata``, which is a simple dict.
The content of this dict has no special meaning to Tiled; it's the user's
space to use or not.

```python
>>> client.metadata  # happens to be empty
DictView({})

>>> client['tables/short_table'].metadata  # happens to have some stuff
DictView({'animal': 'dog', 'color': 'red'})
```

See a later tutorial for how to search Containers with queries.

## Performance Characteristics of Slicing Containers

Consider what Python does when executing this code:

```py
c['x']['y']
```

First, it needs to construct the intermediate result `c['x']`, and then
run `['y']` on that. This requires two HTTP requests, with associated
latency for communication between the client and the server.

Now consider these alternative spellings:

```py
c['x', 'y']
c['x/y']
```

These are not standard dictionary usage; they are Tiled specific short-hands.
They can run faster because they give Python information up front, enabling
Tiled to issue just a single HTTP request to fetch the description of the thing
we actually want (`y`) and avoid spending time fetching `x` in the process.

In some situations, Tiled will proactively bundle the description of `y` into
the description of its parent `x`, in an attempt to reduce back-and-forth. In
those cases, you will find `c['x']['y']` to be just as fast as `c['x', 'y']` or
`c['x/y']`.  This is not possible in all situations, because containers can be
quite large and of course Tiled cannot predict which items the user may need.
