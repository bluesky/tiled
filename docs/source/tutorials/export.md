# Deliberate Export

In this tutorial we will use Tiled to export data in a variety of
common formats for use by some external software (i.e. not Python).

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve demo
```

Now, in a Python interpreter, connect, with the Python client.

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8000")

tables = client["tables"]         # Container of demo tables
images = client["nested/images"]  # Container of demo images
```

The Tiled server can encode its structures in various formats.
These are just a couple of the supported formats:

```python
# Table
tables["short_table"].export("table.xlsx")  # Excel
tables["short_table"].export("table.csv")  # CSV

# Array
images["medium_image"].export("numbers.csv")  # CSV
images["medium_image"].export("image.png")  # PNG image
images["medium_image"].export("image.tiff")  # TIFF image
```

It's possible to select a subset of the data to only "pay" for what you need.

```python
# Export just some of the columns...
tables["short_table"].export("table.csv", columns=["A", "B"])

# Export an N-dimensional slice...
images["medium_image"].export("numbers.csv", slice=[0])  # like arr[0]
import numpy
images["medium_image"].export("numbers.csv", slice=numpy.s_[:10, 100:200])  # like arr[:10, 100:200]
```

In the examples above, the desired format is automatically detected from the
file extension (`table.csv` -> `csv`). It can also be specified explicitly.

```python
# Format inferred from filename...
tables["short_table"].export("table.csv")

# Format given as a file extension...
tables["short_table"].export("table.csv", format="csv")

# Format given as a media type (MIME)...
tables["short_table"].export("table.csv", format="text/csv")
```

## Supported Formats

To list the supported formats for a given structure:

```py
tables["short_table"].formats
```

**It is easy to add formats and customize the details of how they are exported,
so the list of supported formats will vary** depending on whose Tiled service
you are connected to and how it has been configured.

*Out of the box*, Tiled currently supports:

Array:

* C-ordered memory buffer `application/octet-stream`
* JSON `application/json`
* CSV `text/csv`
* PNG `image/png`
* TIFF `image/tiff`
* HTML `text/html`

Table:
* Apache Arrow `application/vnd.apache.arrow.file`
* Parquet `application/x-parqet`
* CSV `text/csv`
* JSON `application/json`
* HTML `text/html`
* Excel (xlsx) `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
* JSON `application/json`
* Newline-delimited JSON `application/json-seq`

Xarray Dataset:
* NetCDF `application/netcdf`
* HDF5 `application/x-hdf`
* The Table formats, by transforming `to_dataframe()`, which may or may not
  be an appropriate transformation depending on your data.

```{note}
The support the full list of formats, the machine that is running `tiled serve ...`
needs to have the relevant I/O libraries installed (e.g. `tifffile` for TIFF,
`pillow` for PNG). If they aren't installed, `tiled serve ...` will detect that
and omit them from the list of supported formats.

**The *user* (client) does *not* need to have any I/O libraries.**
Because the service does all the encoding and just sends opaque bytes for the
client to save, a user can write TIFF files (for example) without actually
having any TIFF-writing Python library installed!
```

## Export to an open file or buffer

It is also possible to export directly into an open file (or any writeable
buffer) in which case the format must be specified.

```python
# Writing directly to an open file
with open("table.csv", "wb") as file:
    tables["short_table"].export(file, format="csv")

# Writing to a buffer
from io import BytesIO

buffer = BytesIO()
tables["short_table"].export(buffer, format="csv")
```

## Limitations

While it is easy to add or change the set exporters, the user does not have
any options for customizing the output of a given exporter. For example, while
the CSV export *does* let the user choose which columns to export, it does
*not* let the user rename the column headings or choose a different value
separator from the default (`,`). Tiled focuses on getting you the precise
data you want, not on formatting it "just so". To do more refined export, use
standard Python tools, as in:

```python
df = tables["short_table"].read()
# At this point we are done with Tiled. From here, we just use pandas,
# or whatever we want.
df.to_csv("table.csv", sep=";", header=["custom", "column", "headings"])
```

Or else add or change the exporters provided by the service to better suit your
needs.

## Consider: Is there a better way?

If your data analysis is taking place in Python, then you may have
no need to export files. Your code will be faster and simpler if you
work directly with numpy, pandas, and/or xarray structures directly.

If your data analysis is in another language, can it access the data
from the Tiled server directly over HTTP? Tiled supports efficient
formats (e.g. numpy C buffers, Apache Arrow Tables) and universal
interchange formats (e.g. CSV, JSON) and perhaps one of those will be the
fastest way to get data into your program.
