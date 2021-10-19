# Search

In this tutorial we will find a dataset in a Node by performing a search
over the entries' metadata.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:tree
```

## Search Using the Python Client

Now, in a Python interpreter, connect with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
```

Tiled has an extensible collection of queries. The client just has to
construct the query, and server sorts out how to execute it as
efficiently as possible given however the metadata and data are stored.

This example collection of data has several entries with metadata.

```python
>>> client['short_table'].metadata
DictView({'animal': 'dog', 'color': 'red'})

>>> client['long_table'].metadata
DictView({'animal': 'dog', 'color': 'green'})

>>> client['structured_data'].metadata
DictView({'animal': 'cat', 'color': 'green'})

# etc.
```

We'll search among them for entries where the term ``"dog"`` appears
anywhere in the metadata.

```python
>>> from tiled.queries import FullText

>>> client.search(FullText("dog"))
<Node {'short_table', 'long_table'}>
```

The result is another client, with a subset of the entries or the original.
We might next stash it in a variable and drill further down.

```python
>>> results = client.search(FullText("dog"))
>>> results['short_table']
<DataFrameClient>
```

Searches may be chained:

```python
>>> client.search(FullText("dog")).search(FullText("red"))
<Node {'short_table'}>
```

If there no matches, the result is an empty Node:

```python
>>> client.search(FullText("something that will not be found"))
<Node {}>
```

## Roadmap

Currently, ``FullText`` is the only outwardly-useful query supported. More
will be added, as well as documentation on how to register user-defined ones.
