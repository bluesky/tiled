# Search

In this tutorial we will find a dataset by performing a search over metadata.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve demo
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
For performance reasons, the search is only performed on a single level
of the nested hierarchy.

This example collection of data has several entries with metadata.

```python
>>> client['tables/short_table'].metadata
DictView({'animal': 'dog', 'color': 'red'})

>>> client['tables/long_table'].metadata
DictView({'animal': 'dog', 'color': 'green'})

>>> client['structured_data'].metadata
DictView({'animal': 'cat', 'color': 'green'})

# etc.
```

We'll search among them for entries where the term ``"dog"`` appears
anywhere in the metadata.

```python
>>> from tiled.queries import FullText

>>> client['tables'].search(FullText("dog"))
<Container {'short_table', 'long_table', 'wide_table'}>
```

The result has a subset of the contents of the original.
Searches may be chained to progressively narrow results:

```python
>>> client['tables'].search(FullText("dog")).search(FullText("red"))
<Container {'short_table', 'wide_table'}>
```

If there no matches, the result is an empty Node:

```python
>>> client.search(FullText("something that will not be found"))
<Container {}>
```

## More Queries

Above examples use the `FullText` query. Tiled supports many queries;
see {doc}`../reference/queries`.
