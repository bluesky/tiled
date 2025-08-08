# Load Slices of Data

In this tutorial we will slice datasets to download only the parts that we
need. We'll also use dask to delay download computation.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve demo
```

Now, in a Python interpreter, connect with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
```

## Slicing Arrays

Navigate to an array (image) dataset in the demo tree.

```python
>>> images = client["nested/images"]  # Container of demo images
>>> print(images)
<Container {'tiny_image', 'small_image', 'medium_image', ...} ~4 entries>
```

```python
>>> images['medium_image']
<ArrayClient shape=(1000, 1000) chunks=((250, 250, 250, 250), (100, 100, ..., 100)) dtype=float64>
```

Slice ``[:]`` to read it. (This syntax may be familiar to h5py users.)

```python
>>> images['medium_image'][:]
array([[0.21074798, 0.39790325, 0.49456221, ..., 0.32959921, 0.34827844,
        0.62495697],
       [0.08099721, 0.78389654, 0.3763025 , ..., 0.76614679, 0.74330957,
        0.66067586],
       [0.74091516, 0.5061625 , 0.00514388, ..., 0.19369308, 0.73790718,
        0.54838425],
       ...,
       [0.36778909, 0.51919955, 0.49982546, ..., 0.32547324, 0.09395182,
        0.5221061 ],
       [0.43235855, 0.53576901, 0.63031618, ..., 0.17347556, 0.89645424,
        0.05639973],
       [0.71051068, 0.43626621, 0.20669469, ..., 0.92879866, 0.49543184,
        0.03710809]])
```

Or, equivalently, use ``images['medium_image'].read()``.

Provide bounds in the slice to download and access just a portion of the
array.

```python
>>> images['medium_image'][:3, 10:15]
array([[0.11429495, 0.64088521, 0.52347248, 0.28147347, 0.60528646],
       [0.82722641, 0.57478402, 0.35443253, 0.34434613, 0.60065387],
       [0.58668817, 0.21471191, 0.05225715, 0.29506593, 0.31148442]])
```

## Slicing Tables

Navigate to a tabular dataset in the demo client.

```python
>>> tables = client["tables"]         # Container of demo tables
>>> print(tables)
<Container {'short_table', 'long_table', 'wide_table'}>
```

```python
>>> tables['short_table']
<DataFrameClient>
```

You can access the columns by listing them.

```python
list(tables['short_table'])
['A', 'B', 'C']
```

You may read it in its entirety like so. Note that table columns may have different
data types.

```python
>>> tables['short_table'].read()
       A         B    C          D      E
index
0      5  0.053270  JJJ 2025-01-01  False
1      9  0.872006  SSS 2025-01-02  False
2      3  0.539313  ttt 2025-01-03  False
3      3  0.597331  fff 2025-01-04   True
4      8  0.038820  AAA 2025-01-05  False
...   ..       ...  ...        ...    ...
95     3  0.935990  aaa 2025-04-06   True
96     6  0.747498  yyy 2025-04-07   True
97     4  0.948744  ccc 2025-04-08  False
98     0  0.764418  EEE 2025-04-09  False
99     9  0.555941  III 2025-04-10   True

[100 rows x 5 columns]
```

You may select a column or a list of columns, and access the column data array directly.

```python
>>> tables['short_table'].read(['A'])
              B
index
0      0.053270
1      0.872006
2      0.539313
3      0.597331
4      0.038820
...         ...
95     0.935990
96     0.747498
97     0.948744
98     0.764418
99     0.555941

[100 rows x 1 columns]

>>> tables['short_table'].read(['A', 'C', 'D'])
       A    C          D
index
0      5  JJJ 2025-01-01
1      9  SSS 2025-01-02
2      3  ttt 2025-01-03
3      3  fff 2025-01-04
4      8  AAA 2025-01-05
...   ..  ...        ...
95     3  aaa 2025-04-06
96     6  yyy 2025-04-07
97     4  ccc 2025-04-08
98     0  EEE 2025-04-09
99     9  III 2025-04-10

[100 rows x 3 columns]

>>> tables['short_table']['A']
<ArrayClient shape=(100,) chunks=((100,)) dtype=uint8>
```

## Dask

[Dask](https://dask.org/) integrates with numpy, pandas, and xarray to enable
advanced parallelism and delayed computation. Configure Tiled to use dask
by passing ``"dask"`` as the second parameter to ``from_uri``.

```python
>>> client = from_uri("http://localhost:8000", "dask")
```

Now use ``client`` the same as above. It will return dask arrays and dataframes.
instead of numpy arrays and pandas ones, respectively. The data is not
immediately downloaded. Only the information about the structure---shape,
datatype(s), internal chunking/partitioning---is downloaded up front.

```python
>>> client["nested/images/big_image"].read()
dask.array<remote-dask-array, shape=(10000, 10000), dtype=float64, chunksize=(4096, 4096), chunktype=numpy.ndarray>
```

```python
>>> client["tables/short_table"].read()
Dask DataFrame Structure:
                   A        B       C              D     E
npartitions=1
               uint8  float64  string  datetime64[s]  bool
                 ...      ...     ...            ...   ...
Dask Name: to_string_dtype, 2 expressions
Expr=ArrowStringConversion(frame=FromMapProjectable(9684b95))
```

Data is downloaded in chunks, in parallel, when ``compute()`` is called.

```python
>>> client["nested/images/big_image"].read().compute()
array([[0.68161254, 0.49255507, 0.00942918, ..., 0.88842556, 0.00536692,
        0.19003055],
       [0.97713062, 0.41684217, 0.62376283, ..., 0.7256857 , 0.61949171,
        0.84613045],
       [0.7604601 , 0.64277859, 0.28309199, ..., 0.0729754 , 0.50716626,
        0.80025002],
       ...,
       [0.98476908, 0.79244797, 0.53337991, ..., 0.23591313, 0.04931968,
        0.91262816],
       [0.63687658, 0.05875549, 0.19458807, ..., 0.2517518 , 0.10880891,
        0.97248376],
       [0.28356223, 0.52545642, 0.7405195 , ..., 0.68566588, 0.25385321,
        0.91432402]])
```

If the dask object is sub-sliced first, only the relevant chunks will be
downloaded.

```python
# This will be fast because it only downloads the relevant chunk(s)
>>> client["nested/images/big_image"].read()[:10, 3:5].compute()
array([[0.26355793, 0.01284164],
       [0.14378819, 0.54898243],
       [0.03100601, 0.88506586],
       [0.05550622, 0.05796535],
       [0.71537642, 0.85890791],
       [0.89535726, 0.99591757],
       [0.64384594, 0.62647887],
       [0.24537111, 0.68344894],
       [0.33606336, 0.03084541],
       [0.319476  , 0.42036447]])
```

As usual with dask, you can perform computations on the "lazy" object and defer
all the actual work to the end.


```python
>>> total = client["nested/images/big_image"].read().sum()
# No data been downloaded yet.

>>> total
dask.array<sum-aggregate, shape=(), dtype=float64, chunksize=(), chunktype=numpy.ndarray>
# No data been downloaded yet.

>>> total.compute()  # Now the data is downloaded and the sum is performed.
50003173.11922723
```
