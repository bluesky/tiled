# Navigating a Catalog

In this tutorial we will navigate a collection of datasets from Python.

To follow along, start the Tiled server with the demo Catalog from a Terminal.

```
tiled serve pyobject tiled.examples.generated:demo
```

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client.catalog import Catalog

catalog = Catalog.from_uri("http://localhost:8000")
```

A Catalog is a nested strucutre of data. Conceptually, it corresponds well to
a directory of files or heirarhcical structure like an HDF5 file.

Tiled provides a utility for visualizing Catalog's nested structure.

```python
>>> from tiled.utils import tree
>>> tree(catalog)
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

Each (sub)catalog displays the names of a couple of its entries---up to
however many fit on one line.


```python
>>> catalog
<Catalog {'arrays', 'dataframes', 'xarrays', 'nested', ...} ~5 entries>
```

Catalogs act like (nested) mappings in Python. All the (read-only) methods
that work on Python dictionaries work on Catalogs. We can lookup a specific
value by its key

```python
>>> catalog['arrays']
<Catalog {'large', 'medium', 'small', 'tiny'}>
```

list all the keys

```python
>>> list(catalog)
['arrays', 'dataframes', 'xarrays', 'nested', 'very_nested']
```

and loop over keys, values, or ``(key, value)`` pairs.

```python
for key in catalog:
    ...

# This is equivalent:
for key in catalog.keys():
    ...

for value in catalog.values():
    ...

for key, value in catalog.items():
    ...
```

Catalogs also support list-like access, via special attributes. This is useful
for efficiently grabbing batches of items, especially if you need to start
from the middle.

```python
>>> catalog.keys_indexer[1:3]  # Access just the keys for entries 1:3.
['dataframes', 'xarrays']

>>> catalog.values_indexer[1:3]  # Access the values (which may be more expensive).
[<Catalog {'df'}>, <Catalog {'large', 'medium', 'small', 'tiny'}>]

>>> catalog.items_indexer[1:3]  # Access (key, value) pairs.
[('dataframes', <Catalog {'df'}>),
 ('xarrays', <Catalog {'large', 'medium', 'small', 'tiny'}>)]
```

Each catalog in the tree has ``metadata``, which is a simple dict.
The content of this dict has no special meaning to Tiled; it's the user's
space to use or not.

```python
>>> catalog.metadata  # happens to be empty
DictView({})

>>> catalog['xarrays'].metadata  # happens to have some stuff
DictView({'description': 'the three main xarray data structures'})
```

See a later tutorial for how to search Catalogs with queries.