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
- Data transfer via SFTP, Globus, etc.

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

## Export to a file

In this section, we tell Tiled how we want the data, and it sends it to us in
that format.

This works:
- No matter what format the data is stored in
- Even if that data isn't even stored in a file at all (e.g., in a database or
  an S3-like blob store)

Let's download an image dataset as a PNG file.

```{code-cell} ipython3
c['examples/images/binary_blobs'].export('my_image.png')
```

We can open the file here or in any other program. It is now just a file on our
local disk.


```{code-cell} ipython3
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

```{tip}
Tiled ships with support for a set of commonly-used formats, and server admins
can add custom ones to meet their users particular requirements.
```

Different data structures support different formats: arrays fit into different
formats than tables do. Let's download the table of edges for carbon from the
xraydb data.

```{code-cell} ipython3
c['examples/xraydb/C/edges'].export('my_table.csv')
```

Again, we can open the file here or in any other program.

```{code-cell} ipython3
!cat my_table.csv
```

## Access as Scientific Python data structures

## Download raw files

## Run a Tiled server

## Upload data

## Register data

Show this from the CLI for now.

## Stream
