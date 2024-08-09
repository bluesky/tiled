# Writing Data

```{warning}

This is a highly experimental feature, recently introduced and included for
evaluation by early users. At this time we do not recommend using it for
anything important.
```

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

Write array and tabular data.

```python
# Write simple Python list (which gets converted to numpy.array).
>>> client.write_array([1, 2, 3], metadata={"color": "red", "barcode": 10})
<ArrayClient shape=(3,) chunks=((3,),) dtype=int64>

# Write an array.
>>> import numpy
>>> client.write_array(numpy.array([4, 5, 6]), metadata={"color": "blue", "barcode": 11})
<ArrayClient shape=(3,) chunks=((3,),) dtype=int64>

# Write a table (DataFrame).
>>> import pandas
>>> client.write_dataframe(pandas.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}), metadata={"color": "green", "barcode": 12})
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

In some scenarios, you may want to write your data a chunk at a time, rather than sending it all at once. This might be in cases where the full data is not available at once, or the data is too large for memory. This can be achieved in two ways:

The first one is to stack them before saving back to client using the above mentioned `write_array` method. This works when the size of data is small.

When the size of merged data becomes an issue for memory, or in cases when you want to save the result on-the-fly as each individual array is generated, this could be achieved by using the `write_block` method with a pre-allocated space in client.

```python
# This approach will require you to know the final array dimension beforehand.

# Assuming you have five 2d arrays (eg. images), each in shape of 32 by 32.
>>> stacked_array_shape = (5, 32, 32)

# Define a tiled ArrayStructure based on shape
>>> import numpy
>>> from tiled.structures.array import ArrayStructure

>>> structure = ArrayStructure.from_array(numpy.zeros(stacked_array_shape, dtype=numpy.int8)) # A good practice to keep the dtype the same as your final results to avoid mismatch.
>>> structure
ArrayStructure(data_type=BuiltinDtype(endianness='not_applicable', kind=<Kind.integer: 'i'>, itemsize=1), chunks=((5,), (32,), (32,)), shape=(5, 32, 32), dims=None, resizable=False)

# Re-define the chunk size to allow single array to be saved.
# In our example, this becomes ((1, 1, 1, 1, 1), (32,), (32,))
>>> structure.chunks = ((1,) * stacked_array_shape[0], (stacked_array_shape[1],), (stacked_array_shape[2],))

# Now to see that the chunk for the first axis has been divided.
>>> structure
ArrayStructure(data_type=BuiltinDtype(endianness='not_applicable', kind=<Kind.integer: 'i'>, itemsize=1), chunks=((1, 1, 1, 1, 1), (32,), (32,)), shape=(5, 32, 32), dims=None, resizable=False)

# Allocate a new array client in tiled
# Note: the following line of code works for tiled version <= v.0.1.0a114
>>> array_client = client.new(structure_family="array", structure=structure, key="stacked_result", metadata={"color": "yellow", "barcode": 13})

# For tiled version >= v0.1.0a115, consider the following
>>> from tiled.structures.data_source import DataSource
>>> data_source = DataSource(structure=structure, structure_family="array")
>>> array_client = client.new(structure_family="array", data_sources=[data_source], key ="stacked_result", metadata={"color": "yellow", "barcode": 13})

>>> array_client
<ArrayClient shape=(5, 32, 32) chunks=((1, 1, 1, 1, 1), (32,), (32,)) dtype=int8>

# Save a single slice with specific index
# Save to the first array (first block index 0)
>>> first_array = numpy.random.rand(32, 32).astype(numpy.int8)
>>> array_client.write_block(first_array, block=(0, 0, 0))

# Save to the 3rd array (first block index 2)
>>> third_array = numpy.random.rand(32, 32).astype(numpy.int8)
>>> array_client.write_block(third_array, block=(2, 0, 0))
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
