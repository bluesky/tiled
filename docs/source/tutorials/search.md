# Search

To follow along, start the Tiled server with the demo Catalog from a Terminal.

```
tiled serve pyobject tiled.examples.generated:demo
```

## Search Using the Python Client

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client.catalog import Catalog

catalog = Catalog.from_uri("http:/localhost:8000")
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
<ClientDaskArrayAdapter>
>>> results['medium']['ones'][:]
dask.array<remote-dask-array, shape=(1000, 1000), dtype=float64, chunksize=(1000, 1000), chunktype=numpy.ndarray>
```

Searches may be chained:

```python
catalog['nested'].serach(FullText("dog")).search(FullText("orange"))
```

If there no matches, the result is an empty catalog:

```python
>>> catalog['nested'].search(FullText("something that will not be found"))
<Catalog {}>
```

## Roadmap

Currently, ``FullText`` is the only outwardly-useful query supported. More
will be added, as well as documentation on how to register user-defined ones.