# Read Custom File Types

In this guide, we will configure Tiled to serve data from custom file types.

```{note}

Tiled is not limited to serving data from files.

Large deployements typically involve a database, supporting fast search on
metadata, and perhaps external files or "blob stores" with large data.

But starting with files is a good way to get rolling with Tiled.
```

## Case 1: Familiar Format, Unfamiliar File Extension

Suppose you have data files that are formatted like CSVs. If they are named
`*.csv` then Tiled can handle them natively without any additional
configuration:

```
tiled serve directory path/to/directory
```

But if they use some unusual extension like `*.stuff` Tiled needs to be
told that it should read `*.stuff` files like CSVs.

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
the [IANA Media Type](https://www.iana.org/assignments/media-types/media-types.xhtml)
also known as a "MIME type" that officially describes this format.

Multiple file extensions can be mapped to the same MIME type. For example,
Tiled's default configuration maps both `.tif` and `.tiff` to `image/tiff`.

We then use the configuration file like this:

```
tiled serve config config.yml
```

The configuration file `config.yml` can be named anything you like.

## Case 2: Familiar Format, No File Extension

Not all files have a name like `<name>.<extension>`. Some have no dot, like:

```
data0001
data0002
data0003
```

Others have a dot, but the part after the dot is not really a file extension;
it does not signify the _format_. Instead, it's scientific metadata of some
kind, as in:

```
polymer_10_new_Ck150V.2050
polymer_10_new_Ck150V.3050
polymer_10_new_Ck150V.4050
```

The best solution is to avoid naming files like this, but we cannot always
control how our files are named. To cope with this, we need to write a
Python function.

```
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
filename or even open the file to, for example, look for a [file
signature](https://en.wikipedia.org/wiki/List_of_file_signatures).  The
function will also be passed the `mimetype`, if any, that was detected based on
its file extension. Therefore, this function can be used to catch files that
have no file extension or to _override_ the determination based file extension
if it is wrong.

If the Python script `custom.py` must be placed in the same directory as
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

The names `custom.py` and `detect_mimetype` are arbitrary. The
`mimetype_detection_hook` may be used in combination with
`mimetypes_by_file_ext`.

## Case 3: Unfamiliar File Format

...

## Summary

Tiled has three ways to identify how to read a file:

* Look up its file extension, e.g. `.csv`, in a built-in registry of common
  file extensions.
* Look up its file extension in a user-provided registry of file extensions.
* Look at the whole file path, and perhaps even open the file, to determine
  what type of file it is.
