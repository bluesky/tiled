# Load Slices of Data

In this tutorial we will slice datasets to download only the parts that we
need. We'll also use dask to delay download computation.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:tree
```

Now, in a Python interpreter, connect with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")
```

## Slicing Arrays

Navigate to an array dataset in the demo tree.

```python
>>> client['medium_image']
<ArrayClient>
```

Slice ``[:]`` to read it. (This syntax may be familiar to h5py users.)

```python
>>> client['medium_image'][:]
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

Or, equivalently, use ``client['medium_image'].read()``.

Provide bounds in the slice to download and access just a portion of the
array.

```python
>>> client['medium_image'][:3, 10:15]
array([[0.11429495, 0.64088521, 0.52347248, 0.28147347, 0.60528646],
       [0.82722641, 0.57478402, 0.35443253, 0.34434613, 0.60065387],
       [0.58668817, 0.21471191, 0.05225715, 0.29506593, 0.31148442]])
```

## Slicing Tables

Navigate to a tabular dataset in the demo client.

```python
>>> client['short_table']
<DataFrameClient ['A', 'B', 'C']>
```

The columns are display in the output. You can also access them
programmatically by listing them.

```python
list(client['short_table'])
['A', 'B', 'C']
```

You may read it in its entirety like so.

```python
>>> client['short_table'].read()
              A         B         C
index
0      0.100145  0.833089  0.381111
1      0.634538  0.061177  0.544403
2      0.838347  0.974533  0.402029
3      0.953260  0.353934  0.019276
4      0.305083  0.048220  0.115531
...         ...       ...       ...
95     0.317265  0.361453  0.602733
96     0.795716  0.341121  0.189589
97     0.620561  0.792025  0.981588
98     0.909704  0.265568  0.576582
99     0.456574  0.918859  0.325529

[100 rows x 3 columns]
```

You may select a column or a list of columns.

```python
>>> client['short_table'].read(['A'])
index
0     0.100145
1     0.634538
2     0.838347
3     0.953260
4     0.305083
        ...
95    0.317265
96    0.795716
97    0.620561
98    0.909704
99    0.456574
Name: A, Length: 100, dtype: float64

>>> client['short_table'].read(['A', 'B'])
              A         B
index
0      0.100145  0.833089
1      0.634538  0.061177
2      0.838347  0.974533
3      0.953260  0.353934
4      0.305083  0.048220
...         ...       ...
95     0.317265  0.361453
96     0.795716  0.341121
97     0.620561  0.792025
98     0.909704  0.265568
99     0.456574  0.918859

[100 rows x 2 columns]
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
>>> client["big_image"].read()
dask.array<remote-dask-array, shape=(10000, 10000), dtype=float64, chunksize=(2500, 2500), chunktype=numpy.ndarray>
```

```python
>>> client["short_table"].read()
Dask DataFrame Structure:
                     A        B        C
npartitions=3
0              float64  float64  float64
34                 ...      ...      ...
68                 ...      ...      ...
99                 ...      ...      ...
Dask Name: remote-dask-dataframe, 3 tasks
```

Data is downloaded in chunks, in parallel, when ``compute()`` is called.

```python
>>> client["big_image"].read().compute()
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
>>> client["big_image"].read()[:10, 3:5].compute()
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
>>> total = client["big_image"].read().sum()
# No data been downloaded yet.

>>> total
dask.array<sum-aggregate, shape=(), dtype=float64, chunksize=(), chunktype=numpy.ndarray>
# No data been downloaded yet.

>>> total.compute()  # Now the data is downloaded and the sum is performed.
50003173.11922723
```
