# Serving a Directory of Files

For this tutorial, install tiffffile.

```
pip install tifffile
```
Generate a directory of TIFF files using a utility provided by Tiled.
(Or use your own, if you have one to hand.)

```
python -m tiled.examples.generate_files some_files
```

This created a directory named ``some_files`` with some files and subdirectories.

```
$ ls some_files
a.tif  b.tif  c.tif  even_more  more
```

The full strucutre looks like

```
$ tree some_files
some_files
├── a.tif
├── b.tif
├── c.tif
├── even_more
│   ├── e.tif
│   └── f.tif
└── more
    └── d.tif

2 directories, 6 files
```

We can serve this directory using Tiled.

```
tiled serve directory some_files
```

Tiled walks the directory, identifies files that it recognizes and has
Readers for. It watches the directory for additions, removals, and changes to
the file.

In a Python interpreter, connect, with the Python client.

```python
from tiled.client.catalog import Catalog

catalog = Catalog.from_uri("http:/localhost:8000")
```

The ``catalog`` has the same tree structure as the directory on
disk, and we can slice and access the data in dask and numpy.

```python
>>> catalog
<Catalog {'more', 'even_more', 'c.tif', 'b.tif', 'a.tif'}>

>>> catalog['more']
<Catalog {'d.tif'}>

>>> catalog['more']['d.tif']
<ClientDaskArrayAdapter>

>>> catalog['more']['d.tif'][:10]
dask.array<getitem, shape=(10, 100), dtype=float64, chunksize=(10, 100), chunktype=numpy.ndarray>
```

Try deleting, moving, or adding files, and notice that the ``catalog`` object
updates its structure.