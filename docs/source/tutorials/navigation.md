# Navigate a Tree

In this tutorial we will navigate a collection of datasets using Tiled's Python
client.

To follow along, start the Tiled server with the demo Tree from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:demo
```

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client import from_uri

tree = from_uri("http://localhost:8000")
```

A Tree is a nested structure of data. Conceptually, it corresponds well to
a directory of files or hierarchical structure like an HDF5 file.

Tiled provides a utility for visualizing Tree's nested structure.

```python
>>> from tiled.utils import tree
>>> tree(tree)
├── arrays
│   ├── large
│   ├── medium
│   ├── small
│   └── tiny
├── dataframes
│   └── df
├── xarrays
│   ├── large
│   │   ├── variable
│   │   ├── data_array
│   │   └── dataset
│   ├── medium
│   │   ├── variable
│   │   ├── data_array
│   │   └── dataset
│   ├── small
│   │   ├── variable
│   │   ├── data_array
│   │   └── dataset
<Output truncated at 20 lines. Adjust tree's max_lines parameter to see more.>
```

Each (sub)tree displays the names of a couple of its entries---up to
however many fit on one line.


```python
>>> tree
<Tree {'arrays', 'dataframes', 'xarrays', 'nested', ...} ~5 entries>
```

Trees act like (nested) mappings in Python. All the (read-only) methods
that work on Python dictionaries work on Trees. We can lookup a specific
value by its key

```python
>>> tree['arrays']
<Tree {'large', 'medium', 'small', 'tiny'}>
```

list all the keys

```python
>>> list(tree)
['arrays', 'dataframes', 'xarrays', 'nested', 'very_nested']
```

and loop over keys, values, or ``(key, value)`` pairs.

```python
for key in tree:
    ...

# This is equivalent:
for key in tree.keys():
    ...

for value in tree.values():
    ...

for key, value in tree.items():
    ...
```

Trees also support list-like access, via special attributes. This is useful
for efficiently grabbing batches of items, especially if you need to start
from the middle.

```python
>>> tree.keys_indexer[1:3]  # Access just the keys for entries 1:3.
['dataframes', 'xarrays']

>>> tree.values_indexer[1:3]  # Access the values (which may be more expensive).
[<Tree {'df'}>, <Tree {'large', 'medium', 'small', 'tiny'}>]

>>> tree.items_indexer[1:3]  # Access (key, value) pairs.
[('dataframes', <Tree {'df'}>),
 ('xarrays', <Tree {'large', 'medium', 'small', 'tiny'}>)]
```

Each tree in the tree has ``metadata``, which is a simple dict.
The content of this dict has no special meaning to Tiled; it's the user's
space to use or not.

```python
>>> tree.metadata  # happens to be empty
DictView({})

>>> tree['xarrays'].metadata  # happens to have some stuff
DictView({'description': 'the three main xarray data structures'})
```

See a later tutorial for how to search Trees with queries.