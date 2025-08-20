# Tiled

Tiled is a **data access** service for data-aware portals and data science tools.
Tiled has a Python client and integrates naturally with Python data science
libraries, but nothing about the service is Python-specific; it also works from
a web browser or any Internet-connected program.

Tiled’s service can sit atop databases, filesystems, and/or remote
services to enable **search** and **structured, chunkwise access to data** in an
extensible variety of appropriate formats, providing data in a consistent
structure regardless of the format the data happens to be stored in at rest. The
natively-supported formats span slow but widespread interchange formats (e.g.
CSV, JSON) and fast, efficient ones (e.g. C buffers, Apache Arrow and Parquet).
Tiled enables slicing and sub-selection to read and transfer only the data of
interest, and it enables parallelized download of many chunks at once. Users can
access data with very light software dependencies and fast partial downloads.

Tiled puts an emphasis on **structures** rather than formats, including:

* N-dimensional strided arrays (i.e. numpy-like arrays)
* Sparse arrays
* Tabular data (e.g. pandas-like "dataframes")
* Nested, variable-sized data (as implemented by [AwkwardArray](https://awkward-array.org/))
* Hierarchical structures thereof (e.g. xarrays, HDF5-compatible structures like NeXus)

Tiled implements extensible **access control enforcement** based on web security
standards, similar to JuptyerHub. Like Jupyter, Tiled can be used by a single
user or deployed as a shared public or private resource. Tiled can be configured
to use third party services for login, such as Google, ORCID. or any OIDC
or SAML authentication providers.

Tiled facilitates **client-side caching** in a standard web browser or in
Tiled's Python client, making efficient use of bandwidth. It uses
**service-side caching** of "hot" datasets and resources to expedite both
repeat requests (e.g. when several users are requesting the same chunks of
data) and distinct requests for different parts of the same dataset (e.g. when
the user is requesting various slices or columns from a dataset).

| Distribution   | Where to get it                                              |
| -------------- | ------------------------------------------------------------ |
| PyPI           | `pip install tiled`                                          |
| Conda          | `conda install -c conda-forge tiled-client tiled-server`     |
| Source code    | [github.com/bluesky/tiled](https://github.com/bluesky/tiled) |
| Documentation  | [blueskyproject.io/tiled](https://blueskyproject.io/tiled)   |

## Example

In this example, we'll serve of a collection of data that is generated in
memory.  Alternatively, it could be read on demand from a directory of files,
network resource, database, or some combination of these.

```
tiled serve demo
# equivalent to:
# tiled serve pyobject --public tiled.examples.generated:tree
```

And then access the data efficiently via the Python client, a web browser, or
any HTTP client.

```python
>>> from tiled.client import from_uri

>>> client = from_uri("http://localhost:8000")

>>> client
<Container {'scalars', 'nested', 'tables', 'structured_data', ...} ~8 entries>

>>> list(client)
['scalars',
 'nested',
 'tables',
 'structured_data',
 'flat_array',
 'low_entropy',
 'high_entropy',
 'dynamic']

>>> client['nested/images/medium_image']
<ArrayClient>

>>> client['nested/images/medium_image'][:]
array([[0.49675483, 0.37832119, 0.59431287, ..., 0.16990737, 0.5396537 ,
        0.61913812],
       [0.97062498, 0.93776709, 0.81797714, ..., 0.96508877, 0.25208564,
        0.72982507],
       [0.87173234, 0.83127946, 0.91758202, ..., 0.50487542, 0.03052536,
        0.9625512 ],
       ...,
       [0.01884645, 0.33107071, 0.60018523, ..., 0.02268164, 0.46955907,
        0.37842628],
       [0.03405101, 0.77886243, 0.14856727, ..., 0.02484926, 0.03850398,
        0.39086524],
       [0.16567224, 0.1347261 , 0.48809697, ..., 0.55021249, 0.42324589,
        0.31440635]])

>>> client['tables/long_table']
<DataFrameClient ['A', 'B', 'C']>

>>> client['tables/long_table'].read()
              A         B         C
index
0      0.246920  0.493840  0.740759
1      0.326005  0.652009  0.978014
2      0.715418  1.430837  2.146255
3      0.425147  0.850294  1.275441
4      0.781036  1.562073  2.343109
...         ...       ...       ...
99995  0.515248  1.030495  1.545743
99996  0.639188  1.278376  1.917564
99997  0.269851  0.539702  0.809553
99998  0.566848  1.133695  1.700543
99999  0.101446  0.202892  0.304338

[100000 rows x 3 columns]

>>> client['tables/long_table'].read(['A', 'B'])
              A         B
index
0      0.246920  0.493840
1      0.326005  0.652009
2      0.715418  1.430837
3      0.425147  0.850294
4      0.781036  1.562073
...         ...       ...
99995  0.515248  1.030495
99996  0.639188  1.278376
99997  0.269851  0.539702
99998  0.566848  1.133695
99999  0.101446  0.202892
```

Using an Internet browser or a command-line HTTP client like
[curl](https://curl.se/) or [httpie](https://httpie.io/) you can download the
data in whole or in efficiently-chunked parts in the format of your choice:

```
# Download tabular data as CSV
http://localhost:8000/api/v1/table/full/tables/long_table?format=csv

# or XLSX (Excel)
http://localhost:8000/api/v1/table/full/tables/long_table?format=xslx

# and subselect columns.
http://localhost:8000/api/v1/table/full/tables/long_table?format=xslx&field=A&field=B

# View or download (2D) array data as PNG
http://localhost:8000/api/v1/array/full/nested/images/medium_image?format=png

# and slice regions of interest.
http://localhost:8000/api/v1/array/full/nested/images/medium_image?format=png&slice=:50,100:200
```

Web-based data access usually involves downloading complete files, in the
manner of [Globus](https://www.globus.org/); or using modern chunk-based
storage formats, such as [TileDB](https://tiledb.com/) and
[Zarr](https://zarr.readthedocs.io/en/stable/) in local or cloud storage; or
using custom solutions tailored to a particular large dataset. Waiting for an
entire file to download when only the first frame of an image stack or a
certain column of a table are of interest is wasteful and can be prohibitive
for large longitudinal analyses. Yet, it is not always practical to transcode
the data into a chunk-friendly format or build a custom tile-based-access
solution. (Though if you can do either of those things, you should consider
them instead!)
