# Writing Data

In this tutorial we will start Tiled in a mode where the client can
write (upload) data for later retrieving, search, or sharing.

To start, we'll launch catalog backed by a _temporary directory_. This is great
for exploring. Then, we'll see how to start one with _persistent_ data, useful
for actual work.

## Quickstart: Launch temporary catalog

```bash
tiled serve catalog --temp
```

You'll see output that includes something like:

```bash

    Navigate a web browser or connect a Tiled client to:

    http://127.0.0.1:8000?api_key=02fa5557fe1c40c410f5e3569cc2b9ec05112770adf6eef068186efec65326a9

```

where the code after `?api_key=` is a randomly-generated secret that will be
different each time you start the server. (Later, you may want to look
at the top of the section {doc}`../explanations/security` for how to control
this.)

Now, in a Python interpreter, connect, with the Python client. Copy/paste in
the full URL.

```python
from tiled.client import from_uri

client = from_uri("http://127.0.0.1:8000?api_key=...")
```

where `...` will be whatever secret was printed at server startup above.

## Write data

Write array.

```python
# Write an array.
>>> import numpy
>>> client.write_array(numpy.array([4, 5, 6]), metadata={"color": "blue", "barcode": 11})
<ArrayClient shape=(3,) chunks=((3,),) dtype=int64>

# Write a Python list (which gets converted to numpy array).
>>> client.write_array([1, 2, 3], metadata={"color": "red", "barcode": 10})
<ArrayClient shape=(3,) chunks=((3,),) dtype=int64>

# Create an array and grow it by one.
>>> new_array = client.write_array([1, 2, 3])
>>> new_array
<ArrayClient shape=(3,) chunks=((3,),) dtype=int64>

# Extend the array. This array has only one dimension, here we extend by one
# along that dimension.
>>> new_array.patch([4], offset=(3,), extend=True)
>>> new_array
<ArrayClient shape=(4,) chunks=((3, 1),) dtype=int64>
>>> new_array.read()
array([1, 2, 3, 4])
```

Write tabular data in a pandas DataFrame.

```python
>>> import pandas
>>> df = pandas.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
>>> client.write_dataframe(df, metadata={"color": "green", "barcode": 12})
<DataFrameClient ['x', 'y']>
```

Search to find the data again.

```py
>>> from tiled.queries import Key
>>> client.search(Key("color") == "green").values().first()
<DataFrameClient ['x', 'y']>
```

Read the data.

```py
>>> client.search(Key("color") == "green").values().first().read()
   x  y
0  1  4
1  2  5
2  3  6
```

## Launch catalog with persistent data

First, we initialize a file which Tiled will use as a database.

```bash
tiled catalog init catalog.db
```

This creates a [SQLite][] file, `catalog.db`. It can be named anything you like
and placed in any directory you like. It will contain the metadata and pointers
to data files.

```bash
tiled serve catalog catalog.db --write data/
```

Now proceed as above. Unlike with `--temp`, the catalog will persist across server
restarts and can be used by multiple servers running in parallel, in a scaled
deployment.

[SQLite]: https://www.sqlite.org/index.html
