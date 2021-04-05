# Search

In this tutorial we will find a dataset in a Catalog by performing a search
over the entries' metadata.

To follow along, start the Tiled server with the demo Catalog from a Terminal.

```
tiled serve pyobject tiled.examples.generated:demo
```

## Search Using the Python Client

Now, in a Python interpreter, connect with the Python client.

```python
from tiled.client.catalog import Catalog

catalog = Catalog.from_uri("http://localhost:8000")
```

Tiled has an extensible collection of queries. The client just has to
construct the query, and server sorts out how to execute it as
efficiently as possible given however the metadata and data are stored.

This subcatalog has four entries.

```python
>>> catalog['nested']
<Catalog {'tiny', 'small', 'medium', 'large'}>
```

Each has different metadata.

```python
>>> catalog['nested']['tiny'].metadata
DictView({'fruit': 'apple', 'animal': 'bird'})

>>> catalog['nested']['small'].metadata
DictView({'fruit': 'banana', 'animal': 'cat'})

# etc.
```

We'll search among them for entries where the term ``"dog"`` appears
anywhere in the metadata.

```python
>>> from tiled.queries import FullText

>>> catalog["nested"].search(FullText("dog"))
<Catalog {'medium'}>
```

The result is another catalog, with a subset of the entries or the original.
We might next stash it in a variable and drill further down.

```python
>>> results = catalog['nested'].search(FullText("dog"))
>>> results['medium']
<Catalog {'ones', 'tens', 'hundreds'}>
>>> results['medium']['ones']
<ClientArrayAdapter>
>>> results['medium']['ones'][:]
array([[0.90346422, 0.88209766, 0.50729484, ..., 0.85845848, 0.40995339,
        0.62513141],
       [0.69748695, 0.30697613, 0.52659964, ..., 0.99122457, 0.45656973,
        0.28431247],
       [0.3397253 , 0.62399495, 0.51621599, ..., 0.17175257, 0.31096683,
        0.72702145],
       ...,
       [0.05031631, 0.04460506, 0.0942693 , ..., 0.7271035 , 0.53009248,
        0.38832301],
       [0.9703186 , 0.59947921, 0.9180047 , ..., 0.30109343, 0.23135718,
        0.10103669],
       [0.05446547, 0.58519701, 0.05065231, ..., 0.60261189, 0.90321973,
        0.89681987]])
```

Searches may be chained:

```python
>>> catalog['nested'].search(FullText("dog")).search(FullText("orange"))
```

If there no matches, the result is an empty catalog:

```python
>>> catalog['nested'].search(FullText("something that will not be found"))
<Catalog {}>
```

## Roadmap

Currently, ``FullText`` is the only outwardly-useful query supported. More
will be added, as well as documentation on how to register user-defined ones.