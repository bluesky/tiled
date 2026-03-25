# Custom Python Client Objects

To provide an "upgraded" and more finely-tuned user experience for certain
kinds of dataset, Tiled can be configured to use custom Python objects.
This is transparent and automatic from the point view of the user.

In the Python client, when a user accesses a given item, Tiled inspects the
item to decide what type of object to use to represent it.
In simple cases, this is just based on the `structure_family`: `"array"` goes
to `tiled.client.array.ArrayClient`;  `"table"` goes to
`tiled.client.dataframe.DataFrameClient`; `"container"` goes to
`tiled.clide.container.Container`. Those classes then manage further communication
with Tiled server to access their contents.

Each item always has exactly one `structure_family`, and it's always from a
fixed list. In addition, it may have a list of `specs`, labels which are meant
to communicate some more specific expectations about the data that may or may
not have meaning to a given client. If a client does not recognize some spec,
it can still access the metadata and data and performed Tiled's essential
functions. If it does recognize a spec, it can provide an upgraded user
experience.

## Example

Suppose data labeled with the `xdi` spec is guaranteed to have a metadata
dictionary containing the following two entries:

```py
x.metadata["XDI"]["Element"]["Symbol"]
x.metadata["XDI"]["Element"]["Edge"]
```

When the Tiled client encounters this type of data, we would like to hand
the user a custom Python object that includes the information in the string
representation displayed by the Python interpreter (or Jupyter notebook).

```py
import tiled.client.dataframe


class XDIDatasetClient(tiled.client.dataframe.DataFrameClient):
    "A custom DataFrame client for DataFrames that have XDI metadata."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Optional sanity check to ensure this cannot be accidentally
        # registered for use with data that is not a table.
        assert self.item["attributes"]["structure_family"] == "table"

    def __repr__(self):
        md = self.metadata["XDI"]
        return f'<{x.item["id"]} {md["Element"]["Symbol"]} {md["Element"]["Edge"]}>'
```

```{note}

**An Aside of Python `__repr__`**

Python calls `__repr__` to obtain a string representation of it for interactive
display as in:

````py
>>> my_object
<...>
````

It is conventional to include angle brackets `<>` when the string is not valid
Python code, as opposed to

````py
>>> 1
1
````

where the string representation is the exactly the code you would run to
reproduce that object.

You may want to override `__str__` in addition, which determines what
is returned by `str(...)` and displayed by `print(...)`. If you override
`__str__` but not `__repr__`, then `__repr__` falls back to `__str__`.
```

Now we want to configure Tiled to this class whenever it encounters
data labeled with the `xdi` spec. We'll register it manually for development
and testing. Then we'll see how to configure it seamlessly for the user.

```py
from tiled.client.container import DEFAULT_STRUCTURE_CLIENT_DISPATCH
from tiled.client import from_uri

custom = dict(DEFAULT_STRUCTURE_CLIENT_DISPATCH["numpy"])
custom["xdi"] = XDIDatasetClient
client = from_uri("https://...", structure_clients=custom)
```

Test by accessing a dataset and checking the type:

```py
type(client[...])
```


Python
[entry points](https://packaging.python.org/en/latest/specifications/entry-points/)
allow Tiled to efficiently scan the software environment for third-party
packages that provide custom Tiled clients. (Crucially, for speed, it does _not_
need to import a package to discover what if any custom Tiled clients it
includes. The entry points declarations can be read statically.) To register a
custom Tiled client from a third party package, add this to the `setup.py`:

```py
# setup.py
setup(
    entry_points={
        "tiled.special_client": [
            "xdi = my_package.my_module:XDIDatasetClient",
        ],
    },
)
```

On the left side of the `=` is the spec name, and on the right is the import
path to the custom object we want Tiled to use. Beware the somewhat unusual
syntax, in particular the colon between the final module and the object.

Re-install the package after adding or editing the entry points. Notice that
even in "editable" installations (i.e. `pip install -e ...`) a re-install step
is needed to register the entry point.

Then, Tiled should be able to automatically discover the custom class with
no change in the user's process:

```py
from tiled.client import from_uri

client = from_uri("https://...")
```

When Tiled see an `xdi` spec, it will query the entry points for a client registered
with that spec. It will discover the custom Python package, import the relevant
class, and use it.

## Precedence

A given item may have multiple specs (or none). It always has exactly one structure family.
It's possible that clients have been registered for multiple specs in the list.
Tiled walks the spec list in order and uses the first one that it recognizes. If it
recognizes none of the specs, or if there are no specs, it falls back to using the
structure family. Specs should generally be sorted from most specific to least
specific, so that Tiled uses the most finely-tuned client object available.

## More Possibilities and Design Guidelines

There are many other useful things we could do with a custom client that is purpose-built
for a specific kinds of data and/or metadata. We can add convenience properties
to quickly access certain metadata.

```py
class CustomClient(...):
    @property
    def element(self):
        return self.metadata["XDI"]["Element"]["Symbol"]
```

We can add convenience methods that read certain sections of the data and perhaps do
light computation on the way out.

```py
class CustomClient(...):
    def energy(self):
        # Read energy column
        return self["energy"][:] * UNIT_CONVERSION
```

We offer two guidelines to help your custom clients compose well with Tiled and
with other scientific Python libraries.

1. In your subclass, _add_ methods, attributes, and properties, but do not
   _change_ the behavior of the existing methods, attributes, and properties in
   the base class. This is a well-known
   [principle](https://en.wikipedia.org/wiki/Liskov_substitution_principle) in
   software design generally, and it is especially crucial here. If a user runs
   code in a software environment where the library with the custom objects
   happens to be missing, we want the user to immediate notice the missing
   methods, not get confusingly different results from the "standard" method
   and the customized one. In addition, it is helpful if the "vanilla" Tiled
   documentation and user knowledge transfers to the custom classes with
   _additions_ but no confusing _changes_.
2. If something custom will do I/O (i.e. download metadata or data from the
   server) make it method, not a property. Properties that do "surprise" I/O
   may block over a slow network and can be very confusing. The same guideline
   applies if the property performs more just very light computation.
