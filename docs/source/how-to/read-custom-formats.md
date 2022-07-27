# Serve Files with Custom Formats

Out of the box, Tiled can serve a directory of files that have common formats
with recognizable file names like `*.csv`, `*.tiff`, or `*.h5`. In this guide,
we will configure it to recognize files that have nonstandard (e.g. custom)
names and/or custom formats.

```{note}

Tiled is not limited to serving data from files.

Large deployements typically involve a database, supporting fast search on
metadata, and perhaps external files or "blob stores" with large data.

But starting with files is a good way to get rolling with Tiled.
```

## Formats are named using "MIME types"

Tiled refers to formats using a web standard called
[MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types).
MIME types look like:

```
text/csv
image/png
application/x-hdf5
```

There is an
[official list](https://www.iana.org/assignments/media-types/media-types.xhtml)
of registered MIME types, and if an official one exists we use it. If
a format is not registered, then the standard tells us to use `text/x-SOMETHING` if the
format is textual or `application/x-SOMETHING` if it is binary. For example,
we use `text/x-xdi` for XDI and `applicaiton/x-hdf5` for HDF5, formats which
are not registered.

## Case 1: Unfamiliar File Extension

Suppose you have data files that are formatted in a supported format like CSVs.
If they were named `*.csv` then Tiled could handle them natively without any
additional configuration:

```
tiled serve directory path/to/directory
```

But if they use some unusual extension like `*.stuff` Tiled needs to be
told that it should read `*.stuff` files like CSVs.

### Map the unfamiliar file extension to a MIME type

We use a configuration file like this:

```yaml
# config.yml
trees:
  tree: files
  args:
    directory: path/to/directory
    mimetypes_by_file_ext:
      .stuff: text/csv
```

We are mapping the file extension, `.stuff` (including the leading `.`) to
the MIME type `text/csv`.

Multiple file extensions can be mapped to the same MIME type. For example,
Tiled's default configuration maps both `.tif` and `.tiff` to `image/tiff`.

We then use the configuration file like this:

```
tiled serve config config.yml
```

The configuration file `config.yml` can be named anything you like.

## Case 2: No File Extension

Not all files have a name like `<name>.<extension>`. Some have no dot, like:

```
data0001
data0002
data0003
```

Others do have a dot, but the part after the dot is not really a file
extension; it does not signify the _format_. Instead, it's scientific metadata
of some kind, as in:

```
polymer_10_new_Ck150V.2050
polymer_10_new_Ck150V.3050
polymer_10_new_Ck150V.4050
```

### Write a custom function for detecting the MIME type

The best solution is to avoid naming files like this, but we cannot always
control how our files are named. To cope with this, we need to write a
Python function.

```python
# custom.py

def detect_mimetype(filepath, mimetype):
    if mimetype is None:
        # If we are here, detection based on file extension came up empty.
        ...
        mimetype = "text/csv"
    return mimetype
```

The function `detect_mimetype` will be passed the full `filepath` (e.g.
`path/to/filename`) not just the filename. It can use this to examine the
filename or even open the file to, for example, look for a
[file signature](https://en.wikipedia.org/wiki/List_of_file_signatures). The
function will also be passed the `mimetype`, if any, that was detected based on
its file extension. Therefore, this function can be used to catch files that
have no file extension or to _override_ the determination based file extension
if it is wrong.

If the Python script `custom.py` is placed in the same directory as
`config.yml`, Tiled will find it. (Tiled temporarily adds the directory
containing the configuration file(s) to the Python import path while
it parses the configuration.)

```yaml
# config.yml
trees:
  tree: files
  args:
    directory: path/to/directory
    mimetype_detection_hook: custom:detect_mimetype
```

Alternatively, if the function can be defined in some external Python package
like `my_package.my_module.func` and configured like

```
mimetype_detection_hook: my_package.my_module:func
```

Note that the packages are separated by `.` but the final object (`func`) is
preceded by a `:`. If you forget this, Tiled will raise a clear error to remind
you.

The names `custom.py` and `detect_mimetype` are arbitrary. The
`mimetype_detection_hook` may be used in combination with
`mimetypes_by_file_ext`.

As in Case 1, we use the configuration file like this:

```
tiled serve config config.yml
```

## Case 3: Custom Format

In this case we format that Tiled cannot read. It's not just a familiar
format with an unfamiliar name; it's a new format that Tiled needs to
be taught how to read.

### Choose a MIME type

Referring back to the top of this guide, we need to choose a MIME type
to refer to this format by. As an example, we'll call our format

```
application/x-stuff
```

The is, of course, some risk of name collisions when we invent names outside of
the
[official list](https://www.iana.org/assignments/media-types/media-types.xhtml)
of MIME types, so be specific.

### Write a custom adapter

Tiled must represent the content of your file as:

* An array + a dictionary of metadata
* A table (dataframe) + dictionary of metadata
* A nested structure (i.e. directory-like hierarchy) of the above

You must choose which is appropriate for this data format. Examples
for each structure follow.

#### Simple Array example

```py
# custom.py
from tiled.adpaters.array import ArrayAdapter

def read_custom_format(filepath):
    # Extract an array and an optional dictionary of metadata
    # from your file.
    array = ...  # a numpy array
    metadata = ...  # a dictionary or None
    return ArrayAdapter.from_array(array, metadata=metadata)
```

#### Simple Tabular (DataFrame) example

```py
# custom.py
from tiled.adpaters.dataframe import DataFrameAdapter

def read_custom_format(filepath):
    # Extract a DataFrame and an optional dictionary of metadata
    # from your file.
    df = ...  # a pandas DataFrame
    metadata = ...  # a dictionary or None
    return DataFrameAdapter.from_pandas(df, npartitions=1, metadata=metadata)
```

#### Simple Nested Structure example

```py
# custom.py
from tiled.adpaters.array import ArrayAdapter
from tiled.adpaters.dataframe import DataFrameAdapter
from tiled.adpaters.mapping import MapAdapter

def read_custom_format(filepath):

    # Build a dictionary (potentially nested) of arrays and/or dataframes.
    # See examples above for ArrayAdapter and DataFrameAdapter usage.

    return MapAdapter(
        {
            "stuff": ArrayAdapter.from_array(...),
            "things": DataFrameAdapter.from_pandas(...),
        }
        metadata={...},
    )
```

#### Advanced: Delay I/O

See the implementations in the pacakage `tiled.adapters` for more advanced
examples, especially ways to refer reading the entire file up front if the user
only wants to read part of it.

#### Advanced: Mark up Structure with optional "Specs"

If the array, table, or nested structure follows some convention or standard
for its internal layout or naming scheme, it can be useful to notate that.
Some Tiled clients may be able to use that information to provide additional
functionality or performance.

See :doc:`../explanations/metadata` for more information on Specs.

Specify them as an argument to the Adapter, as in:

```py
DataFrameAdapter(..., specs=["xdi"])
```

### Configure Tiled to use this Adapter

Our configuration file should use `mimetypes_by_file_ext` (Case 1) or
`mimetype_detection_hook` (Case 2) to recognize this custom file.
Additionally, it should add a section `readers_by_mimetype` to
map our MIME type `application/x-stuff` to our custom function.

Again, Tiled will find `custom.py` if it is placed in the same directory as
`config.yml`. The name is arbitrary, and you can have multiple such files if
needed.

```yaml
# config.yml
trees:
  tree: files
  args:
    directory: path/to/directory
    mimetype_detection_hook: custom:detect_mimetype
    readers_by_mimetype:
      application/x-stuff: custom:read_custom_format
```

We then use the configuration file like this:

```
tiled serve config config.yml
```
