# Tiled Cheat Sheet

```{note}
The audience for this cheat sheet is a scientist reading and writing data in
Tiled from within a Python program.

Not included: Running a Tiled server, or using Tiled's HTTP REST and WebSocket
APIs from other languages.
```


| Install | |
| --|-- |
| pip | `pip install "tiled[client]"` |
| uv | `uv add "tiled[client]"` |
| conda | `conda install -c conda-forge tiled-client` |
| pixi | `pixi add tiled-client` |

**Connect**

```python
# Explicit URL
from tiled.client import from_uri
c = from_uri("https://tiled-demo.nsls2.bnl.gov")

# Alternatively, use a "profile" as shorthand.
from tiled.client import from_profile
c = from_profile("some_alias")
```

**Navigate**

```python
x = c['examples/xraydb']
x.metadata
```

**Search**

```python
from tiled.queries import Key

results = x.search(Key('element.category') == 'nonmetal')
```

**Fetch items**

```python
results.keys().first()
results.keys()[0]  # equivalent

results.values().first()
results.values()[0]  # equivalent

# Loops progressively download results in paginated batches.
for key, value in results.items():
    print(f"~~ {key} ~~")
    print(value.metadata)

# See also last(), head(), tail().
```

**Export**

```python
# Tiled infers desired format from '.csv' file extension
c['examples/xraydb/C/edges'].export('my_table.csv')

# List avaiable formats.
c['examples/images/binary_blobs'].formats
# Specify format explicitly as MIME type.
c['examples/images/binary_blobs'].export('my_image.png', format='image/png')
```

**Slice remotely**

```python
# Download array slice into numpy object in memory.
arr = c['examples/images/binary_blobs'][:50,-50:]

# Download array slice to file on disk.
c['examples/images/binary_blobs'].export(
    'top_right_corner.png',
     slice=np.s_[:50,-50:],
)

# Download table columns into pandas DataFrame in memory.
c['examples/xraydb/C/edges'].read(['edge', 'energy_eV'])

# Download table columns into file on disk.
c['examples/xraydb/C/edges'].export('my_table.csv', columns=['edge', 'energy_eV'])
```

**Locate data sources**

```python
from tiled.client.utils import get_asset_filepaths

# Just the file path(s)
get_asset_filepaths(c['examples/xraydb/C/edges'])

# More detailed information, including format, shape or column names
(as applicable), URIs, etc.
c['examples/xraydb/C/edges'].data_sources()
```
