(custom-export-formats)=
# Add Custom Export Formats

The Tiled server can provide data---in whole or in part---in a variety of
formats. See {doc}`../tutorials/export` for a list of the formats supported out
of the box for each structure family (`"array"`, `"table"`, `"container"`, ...).

This set of formats can easily be extended. A complete working example is
included in the tiled source tree at `example_configs/custom_export_formats`.
We will build it up from scratch.

We'll start with a text-based format and then address a binary one.

## Text Format Example

As our first example, we will invent a variation on CSV (comma-separated variables)
that uses a 🙂 instead of a comma, as in

```
1🙂2🙂3
4🙂5🙂6
```

We will apply this format to arrays. Tiled expects a function with the interface:

```py
def f(
    array: numpy.ndarray,
    metadata: Optional[dict]
): -> str | bytes
    ...
```

Here is an implementation that exports an array as smiley-separated variables.

```py
# custom_exporters.py

def smiley_separated_variables(array, metadata):
    return "\n".join("🙂".join(str(number) for number in row) for row in array)
```

In real-world cases, there is often already a library that writes the format
of interest. Then, our goal isn't to write an exporter from "scratch"; it's to
integrate some existing exporter with Tiled. For example, numpy can be made to
write smiley-separated variables. The trick is to make the library write to a
_buffer in memory_ rather than to a file on disk, and then return a string. Most
libraries support the following approach.

```py
import io
import numpy

def smiley_separated_variables(array, metadata):
    # This StringIO presents a file-like interface that numpy can write to.
    file = io.StringIO()
    numpy.savetxt(buffer, array, delimiter="🙂", fmt="%s")
    return file.getvalue()
```

Either approach---from scratch or using numpy----will work in our case. Notice
that we also get a dictionary of metadata. Some formats give us nowhere to put
this extra information, and we can just drop it in that case.

To integrate this with Tiled, we invoke it in a configuration file.

```yaml
# config.yml

# Register a custom format for the "array" structure family.
media_types:
  array:
    application/x-smileys: custom_exporters:smiley_separated_variables
# And provide some example data to try it with....
trees:
  - path: /
    tree: tiled.examples.generated_minimal:tree
```

The term `application/x-smileys` is a "media type", also known as "MIME type".
In our case, there is no registered
[IANA Media Type](https://www.iana.org/assignments/media-types/media-types.xhtml)
for our exotic format. Therefore, the standard tells us to invent one of the form
`application/x-*`. There is, of course, some risk of name
collisions when we invent names outside of the official list, so be specific.

With `custom_exporters.py` and `config.yml` placed side by side in some
directory, we can start the server.

```
tiled serve config --public config.yml
```

```{note}

If `custom_exporters.py` is placed in the same directory as `config.yml`,
the Tiled server will be able to find and import the `custom_exporters`
module even if it isn't installed in the normal Python module search
path or placed in the current working directory.

When it loads the configuration, Tiled temporarily adds the directory containing
`config.yml` to the Python module search path (`sys.path`). This makes it easy
to prototype and integrate custom code. Of course, the configuration can also
load modules that are installed in the normal fashion.
```

We can request data as smiley-separated variables from the command line
using [HTTPie](https://httpie.io/):

```
$ http :8000/array/full/A?slice=:5,:5 Accept:application/x-smileys
HTTP/1.1 200 OK
content-length: 159
content-type: application/x-smileys; charset=utf-8
date: Wed, 12 Jan 2022 21:38:24 GMT
etag: 8b6ec7a60f30c181762a4c73a6b433b0
server: uvicorn
server-timing: read;dur=3.3, tok;dur=0.1, pack;dur=0.2, app;dur=8.1
set-cookie: tiled_csrf=JDHYkMUIBWECLqIJJvTaEcinv_Vd3kTxS08XCw3N4Yg; HttpOnly; Path=/; SameSite=lax

1.0🙂1.0🙂1.0🙂1.0🙂1.0
1.0🙂1.0🙂1.0🙂1.0🙂1.0
1.0🙂1.0🙂1.0🙂1.0🙂1.0
1.0🙂1.0🙂1.0🙂1.0🙂1.0
1.0🙂1.0🙂1.0🙂1.0🙂1.0
```

or using the Tiled Python client.

```py
from tiled.client import from_uri
c = from_uri("http://localhost:8000/api")
c['A'][:5, :5].export("test.txt", format="application/x-smileys")
```

## Binary Format Example

Let's add support for JPEG images. Tiled doesn't build in support for JPEG; for
scientific uses, PNG is better because it is lossless.

We'll use the library PIL to write the JPEG data. As with the numpy example
above, we need to intercept its output in a buffer. In this case, it will be a
binary buffer, `BytesIO`, instead of `StringIO`.

```py
# custom_exporters.py

import io
from PIL import Image
from tiled.structures.image_serializer_helpers import img_as_ubyte

def to_jpeg(array, metadata):
    # PIL detail: ensure array has compatible data type before handing to PIL.
    prepared_array = img_as_ubyte(array)
    image = Image.fromarray(prepared_array)
    file = io.BytesIO()
    image.save(file, format="jpeg")
    return file.getbuffer()
```

This covers the basic functionality. See the built-in exporters in
`tiled/structures/array.py` for details that add polish, like scaling the
image's dynamic range and failing gracefully when given arrays that have the
wrong dimensionality to be exported as an image.

We'll add it to our configuration.

```yaml
# config.yml

media_types:
  array:
    application/x-smileys: custom_exporters:smiley_separated_variables
    image/jpeg: custom_exporters:to_jpeg
trees:
  - path: /
    tree: tiled.examples.generated_minimal:tree
```

Start the server again

```
tiled serve config --public config.yml
```

and navigate a web browser to `http://localhost:8000/api/v1/array/full/A?format=image/jpeg`.
Since the example data is just an array of ones, this will appear as a white square image.

## File extensions as convenience aliases

Now, `image/jpeg` is unwieldy for users unfamiliar with MIME types. Adding

```yaml
file_extensions:
  jpg: image/jpeg
  jpeg: image/jpeg
```

to the configuration enables

```
http://localhost:8000/api/v1/array/full/A?format=jpeg
http://localhost:8000/api/v1/array/full/A?format=jpg
```

as equivalent to

```
http://localhost:8000/api/v1/array/full/A?format=image/jpeg
```

```{note}

The format can also be specified as an
[HTTP `Accept` Header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept).
In that case, it must be given as a MIME type, in accordance with the standard.
The file extension alias is not accepted.
```

## Advanced: Streaming export

HTTP supports chunked responses, where data is streamed incrementally. This
is a good fit for streaming-oriented formats such as newline-delimited JSON.

To create a chunked exporter, implement your exporter as a Python generator
that yields bytes.

```python
def export(array, metadata):
    for ... in ...:
        yield b"..."
```

## Further examples

At the bottom of each of the modules in `tiled/structures`, you will
find the code for the built-in exporters (a.k.a "serializers").
