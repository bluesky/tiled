# Serve Data as Zarr

Tiled allows one to serve their data in the [zarr](https://zarr.dev/) format. Even though internally the data might be stored in any format supported by Tiled, they can be presented to a client in the standard zarr interface. This enables one to use any compatible external tools, for example, for fast (de-)serialization and intercative plotting of multidimensional arrays.

Tiled supports Zarr specification versions 2 and 3, which are exposed at the `/zarr/v2` and `/zarr/v3` API endpoints, respectively. The native Tiled datastructures are mapped to zarr as follows:

| Tiled | zarr |
|------|-----|
| Container | Group |
| Array | Array |
| Sparse Array| Array (dense) |
| Data Frame | Group (of columns) |
| Data Frame Column | Array |


## Examples

In the following examples, we use the local demo Tiled server exposed on `127.0.0.1:8000` with the `tiled serve demo` command and demonstrate how it can be integrated into workflows that rely on zarr.


### Scenario 1. Use Python Client for Zarr

This example demonstrates the use of the Python zarr client, for example in a script, an interactive Python shell, or a Jupyter notebook. To access the Tiled data, one can read the server contents into zarr via a file system mapper:

```python
import zarr
from fsspec import get_mapper

url = "http://localhost:8000/zarr/v3/"
fs_mapper = get_mapper(url)
root = zarr.open(fs_mapper, mode="r")
```

The resulting object is a zarr.Group, which represents the root of the Tiled catalog tree and supports (most) of the usual operations on zarr groups:

```python
>>> print(group)
<zarr.hierarchy.Group '/' read-only>

>>> list(group.keys())
['dynamic', 'flat_array', 'high_entropy', 'low_entropy',
'nested', 'scalars', 'structured_data', 'tables']
```

```python
>>> root.tree()
/
 ├── dynamic (3, 3) float64
 ├── flat_array (100,) float64
 ├── low_entropy (100, 100) int32
 ├── nested
 │   ├── cubes
 │   │   ├── tiny_cube (50, 50, 50) float64
 │   │   └── tiny_hypercube (50, 50, 50, 50, 50) float64
 │   ├── images
 │   │   ├── big_image (10000, 10000) float64
 │   │   ...
 │   │   └── tiny_image (50, 50) float64
 │   └── sparse_image (100, 100) float64
 ├── scalars
 │   ...
 │   └── pi () float64
 ├── structured_data
 │   ├── pets
 │   └── xarray_dataset
 │       ├── lat (2, 2) float64
 │       ├── temperature (2, 2, 3) float64
 │       ...
 │       └── time (3,) datetime64[ns]
 └── tables
     ├── long_table
     │   ...
     │   └── C (100000,) float64
     └── wide_table
         ├── A (10,) float64
         ...
         └── Z (10,) float64
```

> **_NOTE:_**  To access Tiled servers that require authentication, one can pass an api-key in the header of the HTTP requests. With `fsspec`, this is done by explicitly constructing an `HTTPFileSystem` object and mapping it to zarr:
> ```python
> from fsspec.implementations.http import HTTPFileSystem
>
> headers = {"Authorization": "Apikey your-api-key-goes-here",
>            "Content-Type": "application/json"}
> fs = HTTPFileSystem(client_kwargs={"headers": headers})
> root = zarr.open(fs.get_mapper(url), mode="r")
> ```


### Scenario 2. Data Visualization with a Web-based Browser Client

Web-based visualization clients offer fast rendering of large datasets without the need to install any dependencies. As an example, we show how data from Tiled can be visualized in [vizarr](https://github.com/hms-dbmi/vizarr) directly via the `/zarr` endpoints.

> **_NOTE:_**  To allow the Tiled server to accept requests from the web application, its domain [must be declared](https://blueskyproject.io/tiled/reference/service-configuration.html#allow-origins) in `allow_origins` section of the server config. When using the local demo server, start it with the appropriate value of the `TILED_ALLOW_ORIGINS` environmet variable, e.g.
> ```
> TILED_ALLOW_ORIGINS='["https://hms-dbmi.github.io"]' tiled serve demo
> ```

Once, the server is running, navigate to the following url to dispaly the image in a web browser:
https://hms-dbmi.github.io/vizarr/?source=http://127.0.0.1:8000/zarr/v2/nested/images/medium_image.
