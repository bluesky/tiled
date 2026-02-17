---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.17.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# 10 minutes to Tiled

This is a short, tutorial-style introduction to Tiled, for new users.

## Connect

To begin, we will use a public demo instance of Tiled. If you are reading this
tutorial without an Internet connection, see the section below on running your
own Tiled server on your laptop.

This tutorial focuses on accessing Tiled from Python. But you can also interact
with Tiled from your web browser by navigating to
[https://tiled-demo.nsls2.bnl.gov](https://tiled-demo.nsls2.bnl.gov) where
you'll find a web-based user interface and more.


```{code-cell} ipython3
from tiled.client import from_uri

c = from_uri("https://tiled-demo.nsls2.bnl.gov")
```

```{note}
At this point, some Tiled servers might prompt you to **log in** with a username
and password. But the demo we are using here is configured to allow **public**,
anonymous access.
```

## Navigate

Tiled holds its data in a directory-like "container".  Here we see the names of
the entries it contains.

```{code-cell} ipython3
c
```

Let's look inside "examples"—another container.

```{code-cell} ipython3
c['examples']
```

When the container is large, we see the first several entries.

```{code-cell} ipython3
c['examples/xraydb']
```

````{tip}

These are equivalent, but the first two run faster.

```python
# fast
c['examples/xraydb']
c['examples', 'xraydb']

# slower because Python cannot "see ahead"
c['examples']['xraydb']
```

````

## Search on metadata

Every entry in Tiled has metadata, which we can access via the `metadata`
attribute. Tiled does not enforce any requirements on this general; it's
arbitrary JSON.

Let's take a peek at the first entry to get a sense of what we might
search for in this case.

```{code-cell} ipython3
x = c['examples/xraydb']
x.values().first().metadata
```

- Search filters look like `Key('my.nested.metadata') == 'my value')1.
- Can also use `>`, `<`, `>=`, `<=`.
- And a lot more...

```{code-cell} ipython3
from tiled.queries import Key

x.search(Key('element.category') == 'nonmetal')
```

Queries can be chained to progressively narrow results.

```{code-cell} ipython3
x.search(
    Key('element.category') == 'nonmetal'
).search(
    Key('element.atomic_number') < 16
)
```

What other values does `element.category` take? We could answer that question
by downloading all the entries and tabulating them in Python, but it's faster
to ask Tiled to do it and just send the answer.

```{code-cell} ipython3
x.distinct('element.category', counts=True)['metadata']
```

We can stash the results in a variable and access them in various ways.

```{code-cell} ipython3
results = x.search(Key('element.category') == 'noble_gas')
print(f"Noble gases in this data set: {list(results)}")
```

We can efficiently access only the first result without downloading the
metadata for _all_ the results.

```{code-cell} ipython3
first_result = results.values().first()
first_result.metadata
```

````{tip}

Try these:

```python
results.keys().first()
results.keys().last()
results.keys()[2]
results.keys()[:3]

results.values().first()
results.values().last()
results.values()[2]
results.values()[:3]

for key, value in results.items():
    print(f"~~ {key} ~~"
    print(value.metadata)
```

````

## Locate data sources (e.g., files)

Once you have identified data sets of interest, Tiled can point to where the
underlying data is stored. You can then access them by any convenient means:

- Direct filesystem access
- File transfer via SFTP, Globus, etc.
- File transfer via Tiled

Here we'll see the file that backs the table of Carbon edges in our xraydb
dataset.

```{code-cell} ipython3
from tiled.client.utils import get_asset_filepaths

get_asset_filepaths(c['examples/xraydb/C/edges'])
```

Tiled knows a whole lot more than just the file path. The data dump below
includes the format (`mimetype`) of the data, its `structure`, and other
machine-readable information that is necesasry for applications to navigate the
file and load the data.

```{code-cell} ipython3
ds, = c['examples/xraydb/C/edges'].data_sources()
ds
```

Now the data may not be stored in a file at all. Tiled understands data
stores in databases or S3-like blob stores as well, and these are become
increasingly common for as data scales and moves into cloud environments.

The data location is always given as a URL. That URL begins with `file://` if
it's a plain old file or something else if it is not.

```{code-cell} ipython3
ds.assets[0].data_uri
```

## Access as Scientific Python data structures

Tiled can download data directly into scientific Python data structure, such as
**numpy**, **pandas**, and **xarray**.
_This is how we encourage Python users to use Tiled for analysis._
It has several advantages:

- No need to name or organize files.
- No need to wait for disk: load the data straight from the network into your
  data analsysis. (Disks are often the slowest things we deal with in
  computing.)

```{code-cell} ipython3
c['examples/xraydb/C/edges']
```

```{code-cell} ipython3
c['examples/xraydb/C/edges'].read()
```

```{code-cell} ipython3
c['examples/images/binary_blobs']
```

```{code-cell} ipython3
arr = c['examples/images/binary_blobs'].read()
arr
```

```{code-cell} ipython3
%matplotlib inline
import matplotlib.pyplot as plt

plt.imshow(arr)
```

## Export to a preferred format

In this section, we tell Tiled how we want the data, and it sends it to us in
that format.

This works:
- No matter what format the data is stored in
- Even if that data isn't even stored in a file at all (e.g., in a database or
  an S3-like blob store)

Let's download the table of edges for carbon from the xraydb data.

```{code-cell} ipython3
# Download as Excel spreadsheet
c['examples/xraydb/C/edges'].export('my_table.xlsx')

# Or, download as CSV file
c['examples/xraydb/C/edges'].export('my_table.csv')
```

We can open the files here or in any other program. They are now just files on our
local disk.

```{code-cell} ipython3
!cat my_table.csv
```

Let's download an image dataset as a PNG file.

```{code-cell} ipython3
c['examples/images/binary_blobs'].export('my_image.png')
```

Again, we can open the file here or in any other program.

```{code-cell} ipython3
:tags: [hide-input]

from IPython.display import Image

Image(filename='my_image.png')
```

Tiled tries to recognize the file format you want from the file extension, as
in `my_file.png` above. It can be also be specified explicitly using:

```{code-cell} ipython3
c['examples/images/binary_blobs'].export('my_image.png', format='image/png')
```

We can review the file formats.

```{code-cell} ipython3
c['examples/images/binary_blobs'].formats
```

Different data structures support different formats: arrays fit into different
formats than tables do.

```{code-cell} ipython3
c['examples/xraydb/C/edges'].formats
```

```{tip}
Tiled ships with support for a set of commonly-used formats, and server admins
can add custom ones to meet their users particular requirements.
```

## Download raw files

Sometimes it is best to just download the files exactly as they were. This may
be the most convenient thing, or it may be necessary to comply with transparency
requirements that mandate providing a byte-for-byte copy of the raw data.

As shown above, Tiled can provide the filepaths, and you can fetch the files
by any available means. Tiled can also download the files directly. It does
this efficiently by launching parallel downloads.


```{code-cell} ipython3
c['examples/xraydb/C/edges'].raw_export('downloads/')
```

## Run a Tiled server

Up to this point, we've been reading from Tiled's public demo instance. To
demonstrate writing data, we'll need our own server because the public demo
doesn't allow us to write. (If you already have access to an institutional
Tiled server that grants you write access, feel free to use that!) The simplest
way to get started is to launch a local server with embedded storage and basic
security:

```{code-cell} ipython3

from tiled.client import from_catalog

c = from_catalog('example')
```

The server prints a URL when it starts. Your URL will differ: each launch
generates a unique secret `api_key`. You can paste this URL into a browser to
open Tiled's web interface.

```{tip}
This embedded setup is convenient for experimentation but isn't designed for
production or multi-user use. For robust, scalable deployments, see the user guide.
```

## Upload data

We now have an empty Tiled server that we can _write_ into.

```{code-cell} ipython3
ac = c.write_array([1, 2, 3])
ac.read()
```

We can optionally include metadata and/or give it a name, a `key`.
(By default it gets a long random one.)


```{code-cell} ipython3
ac = c.write_array(
    [1, 2, 3],
    metadata={'color': 'blue'},
    key='hello'
)
ac.metadata
```

We can find it via search.

```{code-cell} ipython3
c.search(Key('color') == 'blue')
```

Similarly, we can upload tabular data.

```{code-cell} ipython3
tc = c.write_table({'a': [1, 2, 3], 'b': [4, 5, 6]})
tc.read()
```

We can organize items in to nested containers.

```{code-cell} ipython3
c.create_container('x')
c['x'].write_array([1,2,3], key='a')
c['x'].write_array([4,5,6], key='b')
c['x']
```

## Stream
