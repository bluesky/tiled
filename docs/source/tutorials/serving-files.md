# Serve a Directory of Files

In this tutorial, we will use Tiled to browse a directory of 
spreadsheets and image files from Python and read the data as pandas
DataFrames and numpy arrays.

For this tutorial, install tiffffile and openpyxl.

```
pip install tiled[complete]
pip install tifffile openpyxl
```

Generate a directory of example files using a utility provided by Tiled.
(Or use your own, if you have one to hand.)

```
python -m tiled.examples.generate_files example_files/
```

This created a directory named ``example_files`` with some files and subdirectories.

```
$ ls example_files
a.tif  b.tif  c.tif  even_more  more
```

The full structure looks like

```
├── another_table.csv
├── a.tif
├── b.tif
├── c.tif
├── even_more
│   ├── e.tif
│   └── f.tif
├── more
│   └── d.tif
└── tables.xlsx
```

We can serve this directory using Tiled.

```
tiled serve directory --public example_files
```

Tiled walks the directory, identifies files that it recognizes and has
Readers for. It watches the directory for additions, removals, and changes to
the file.

In a Python interpreter, connect with the Python client.

```python
from tiled.client import from_uri

tree = from_uri("http://localhost:8000")
```

The ``tree`` has the same tree structure as the directory on
disk, and we can slice and access the data.

```python
>>> client
<Tree {'more', 'even_more', 'b.tif', 'a.tif', 'c.tif', ...} ~7 entries>

>>> client['more']
<Tree {'d.tif'}>

>>> client['more']['d.tif']
<ArrayClient>

>>> client['more']['d.tif'].read()
array([[1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       ...,
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.],
       [1., 1., 1., ..., 1., 1., 1.]])

>>> client['tables.xlsx']
<Tree {'Sheet 1', 'Sheet 2'}>

>>> client['tables.xlsx']['Sheet 1']
<DataFrameClient ['A', 'B']>

>>> client['tables.xlsx']['Sheet 1'].read()
   A  B
0  1  4
1  2  5
2  3  6
```

Try deleting, moving, or adding files, and notice that the ``client`` object
updates its structure. It continually watches the filesystem for changes in an
efficient fashion.