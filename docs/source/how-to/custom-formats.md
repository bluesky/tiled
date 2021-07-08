# Add Support for a Custom Format

For a given "structure family" (e.g. `array`, `dataframe`) the Tiled server
can provide the data---in whole or in part---in a variety of formats. 
See {doc}`../tutorials/export` for a list of the formats supported out of the
box.

To teach Tiled to serve another format, first decide which structure family or
families are appropriate for this format? For example, it make sense to export
an array as an image, but it does not make sense to export a table as an
image---or at least, if you do, you are making some assumptions about the table.

For this guide, we'll take the example of
[XDI](https://github.com/XraySpectroscopy/XAS-Data-Interchange/blob/master/specification/spec.md#example-xdi-file),
which is a formalized text-based format for X-ray Spectroscopy data. As you can
see from the linked example, the file's contents comprise a single table
and a header of key--value pairs. Therefore, it makes sense to export `dataframe`
structures and their associated metadata into this format.

Take the following simple server configuration:

```yaml
# config.yml
trees:
- path: /
  tree: tiled.examples.xas:tree
```

and serve it:

```
tiled serve config --public config.yml
```

As is, we can access the data as CSV, for example.

```
$ curl -H 'Accept: text/csv' http://localhost:8000/dataframe/full/example
energy,i0,itrans,mutrans
8779.0,149013.7,550643.089065,-1.3070486
8789.0,144864.7,531876.119084,-1.3006104
8799.0,132978.7,489591.10592,-1.3033816
8809.0,125444.7,463051.104096,-1.3059724
8819.0,121324.7,449969.103983,-1.3107085
8829.0,119447.7,444386.117562,-1.3138152
8839.0,119100.7,440176.091039,-1.3072055
8849.0,117707.7,440448.106567,-1.3195882
8859.0,117754.7,442302.10637,-1.3233895
8869.0,117428.7,441944.116528,-1.3253521
8879.0,117383.7,442810.120466,-1.327693
8889.0,117185.7,443658.11566,-1.3312944
```

```{note}
There are three equivalent ways to request a format, more formally called a "media type" or a "MIME type".

    1. Use the standard [HTTP `Accept` Header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept).

    ```
    $ curl -H 'Accept: text/csv' http://localhost:8000/dataframe/full/example
    ```

    2. Place the media type in a `format` query parameter.

    ```
    $ curl -H http://localhost:8000/dataframe/full/example?format=text/csv
    ```

    3. Provide just a file extension. This is user friendly for people who do not know or care what
    a "media type" is. The serve looks up `csv` in a registry mapping file extensions to media types.

    ```
    $ curl http://localhost:8000/dataframe/full/example?format=csv
    ```

```

## Define an exporter

When a client requests an XDI-formatted response, the Tiled server
will call our custom exporter with two arguments: the structure itself
(in this case, a `pandas.DataFrame`) and a dictionary of metadata.
The metadata is freeform as far as Tiled is concerned---its content
and any internal structure is completely up to the user---so if we
have special requirements about what it must contain, we need to
do that validation inside our exporter. We might, for example,
refuse to export (raise an error) if required fields are missing
from the metadata or if the DataFrame we are given does not have the
expected columns.

The exporter must return either `str` or `bytes`.

```py
# exporter.py

def serialize_xdi(dataframe, metadata):
    return f"""
STUFF
{metadata}
{dataframe.to_csv()}
"""
```

## Register the exporter

Add new sections to the configuration as follows.

```yaml
trees:
- path: /
  tree: tiled.examples.xas:tree
media_types:
  dataframe:
    application/x-xdi: exporter:serialize_xdi
file_extensions:
  xdi: application/x-xdi
```

First consider

```yaml
media_types:
  dataframe:
    application/x-xdi: exporter:serialize_xdi
```

The key, `application/x-xdi` must be a valid media type. If there is no
registered [IANA Media Types](https://www.iana.org/assignments/media-types/media-types.xhtml)
for the format of interest (as is the case here), the standard tells us
to invent one of the form `application/x-YOUR-NAME-HERE`. There is, of course,
some risk of name collisions when we invent names outside of the official list,
so be specific.

The final section

```yaml
file_extensions:
  xdi: application/x-xdi
```

enables the usage

```
$ curl http://...?format=xdi
```

by mapping `"xdi"` to the media type.


The value, `exporter:serialize_xdi`, is the module or package that our
exporter is defined in, followed by `:` and then the function name.

The Python file where `serialize_xdi` is defined by must in an importable location.
During configuration-parsing, Tiled *temporarily* adds the directory containing
the config file itself to `sys.path`. This means that we can conveniently
drop `exporter.py` next to `config.yml` and know that it will be found.
For long-term deployments it's better to place exporters in installable Python
packages (i.e. with a proper `setup.py`, etc.) but for prototyping and
development this is much more expedient.

Now if we restart the server again with this updated `config.yml`

```
tiled serve config --public config.yml
```

we can request the content as XDI in any of these ways:

```
$ curl -H 'Accept: application/x-xdi' http://localhost:8000/dataframe/full/example
$ curl http://localhost:8000/dataframe/full/example?format=application/x-xdi
$ curl http://localhost:8000/dataframe/full/example?format=xdi
```