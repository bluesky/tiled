# Serve Files with Custom Formats

Out of the box, Tiled can serve a directory of files that have common formats
with recognizable file names like `*.csv`, `*.tiff`, or `*.h5`. In this guide,
we will configure it to recognize files that have nonstandard (e.g. custom)
names and/or custom formats.

```{note}

Tiled is not limited to serving data from files.

Large deployments typically involve a database, supporting fast search on
metadata, and perhaps external files or "blob stores" with large data.

But starting with files is a good way to get rolling with Tiled.
```

## Formats are named using "MIME types"

Tiled refers to formats using a web standard called
[MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types)
a.k.a. "media types".
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


```
tiled serve directory path/to/directory --ext '.stuff=text/csv'
```

We are mapping the file extension, `.stuff` (including the leading `.`) to
the MIME type `text/csv`.

Multiple file extensions can be mapped to the same MIME type. For example,
Tiled's default configuration maps both `.tif` and `.tiff` to `image/tiff`.
Multiple custom mapping can be specified by using `--ext` repeatedly.

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

Place `custom.py` in the current working directory and reference it like this:

```
tiled serve directory path/to/directory --mimetype-hook custom:detect_mimetype
```

* The names `custom.py` and `detect_mimetype` are arbitrary.
* The function may be in the any importable location; it does not have to be
  in the current working directory. Functions in nested packages can referenced
  like `package.module.submodule:function_name`. Notice the `.`s between
  modules and the `:` before the function.
* The `--mimetype-hook` may be used in combination with `--ext` above.

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
* A table + dictionary of metadata
* A nested structure (i.e. directory-like hierarchy) of the above

You must choose which is appropriate for this data format. Examples
for each structure follow.

#### Simple Array example

```py
# custom.py
from tiled.adapters.array import ArrayAdapter

def read_custom_format(filepath, metadata=None, **kwargs):
    # Extract an array and an optional dictionary of metadata
    # from your file.
    array = ...  # a numpy array
    if metadata is None:
        metadata = ...  # a dictionary or None
    return ArrayAdapter.from_array(array, metadata=metadata, **kwargs)
```

#### Simple Tabular example

```py
# custom.py
from tiled.adapters.table import TableAdapter

def read_custom_format(filepath, metadata=None, **kwargs):
    # Extract a DataFrame and an optional dictionary of metadata
    # from your file.
    df = ...  # a pandas DataFrame
    if metadata is None:
        metadata = ...  # a dictionary or None
    return TableAdapter.from_pandas(df, npartitions=1, metadata=metadata, **kwargs)
```

#### Simple Nested Structure example

```py
# custom.py
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.table import TableAdapter
from tiled.adapters.mapping import MapAdapter

def read_custom_format(filepath, metadata=None, **kwargs):

    # Build a dictionary (potentially nested) of arrays and/or tables.
    # See examples above for ArrayAdapter and TableAdapter usage.

    if metadata is None:
        metadata = ...  # a dictionary or None
    return MapAdapter(
        {
            "stuff": ArrayAdapter.from_array(...),
            "things": TableAdapter.from_pandas(...),
        }
        metadata=metadata,
	**kwargs,
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
TableAdapter.from_pandas(..., specs=["xdi"])
```

### Configure Tiled Server to use this Adapter

Our configuration file should use `mimetypes_by_file_ext` (Case 1) or
`mimetype_detection_hook` (Case 2) to recognize this custom file.
Additionally, it should add a section `adapters_by_mimetype` to
map our MIME type `application/x-stuff` to our custom function.

Again, Tiled will find `custom.py` if it is placed in the same directory as
`config.yml`. The name is arbitrary, and you can have multiple such files if
needed.

```yaml
# config.yml
trees:
  - tree: catalog
    path: /
    args:
      uri: ./catalog.db
      readable_storage:
        - path/to/directory
      adapters_by_mimetype:
        application/x-stuff: custom:read_custom_format
```

We then use the configuration file like this:

```
tiled serve config config.yml --api-key secret
```

and register the files in a separate step. Use `--ext` and/or `--mimetype-hook`
described above to register files as your custom MIME type (e.g.
`application/x-stuff`). For example:


```
tiled register http://localhost:8000 \
  --api-key secret \
  --verbose \
  --ext '.stuff=application/x-stuff' \
  --adapter 'application/x-stuff=custom:read_custom_format' \
  path/to/directory
```
