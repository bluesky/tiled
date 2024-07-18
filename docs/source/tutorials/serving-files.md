# Serve a Directory of Files

In this tutorial, we will use Tiled to browse a directory of
spreadsheets and image files from Python and read the data as pandas
DataFrames and numpy arrays.

Generate a directory of example files using a utility provided by Tiled.
(Or use your own, if you have one to hand.)

```
python -m tiled.examples.generate_files example_files/
```

This created a directory named ``example_files`` with some files and subdirectories.

```
$ ls example_files
another_table.csv  a.tif  b.tif  c.tif  more  tables.xlsx
```

The full structure looks like

```
├── another_table.csv
├── a.tif
├── b.tif
├── c.tif
├── more
│   ├── A0001.tif
│   ├── A0002.tif
│   ├── A0003.tif
│   ├── B0001.tif
│   ├── B0002.tif
│   └── even_more
│       ├── e.tif
│       └── f.tif
└── tables.xlsx
```

We can serve this directory using Tiled.

```
tiled serve directory --public example_files
```

Tiled walks the directory, identifies files that it recognizes and has
Readers for. It can watch the directory for additions, removals, and changes to
the file with option `--watch`.

In a Python interpreter, connect with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
```

The ``client`` has the same tree structure as the directory on
disk, and we can slice and access the data.

```python
>>> client
<Container {'another_table', 'tables', 'c', 'a', 'b', 'more'}>

>>> client['more']
<Container {'A', 'B', 'even_more'}>

>>> client['more']['A']
<ArrayClient shape=(3, 100, 100) chunks=((1, 1, 1), (100,), (100,)) dtype=float64>

>>> client['more']['A'][0]
array([[1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       ...,
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.]])

>>> client['tables']
<Container {'Sheet 1', 'Sheet 2'}>

>>> client['tables']['Sheet 1']
<DataFrameClient>

>>> client['tables']['Sheet 1'].read()
   A  B
0  1  4
1  2  5
2  3  6
```

The usage `tiled serve directory ...` is mostly for demos and small-scale use.
For more sophisticated control over this process, see {doc}`../how-to/register`.
