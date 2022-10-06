# Metadata, Specs, and References

## Metadata

Each Tiled node carries an optional dictionary of metadata. This is fully
under the control of the user: Tiled itself is not opinionated about its
content and does not reserve any names for its own use.

```{note}
The metadata dictionary may be arbitrarily nested, and it may contain
anything that Tiled can transmit as JSON or [msgpack](https://msgpack.org).
All JSON-serializable types (dicts, lists, numbers, strings) are supported.
The following types are additionally supported:

* Dates, represented as `datetime.datetime` objects,
  are supported natively by msgpack. JSON does not support dates, so they are
  converted to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) strings
  when clients request JSON.
* Numpy objects are tolerated as input, but they are converted to (nested)
  lists before encoding to msgpack or JSON. Metadata is not a suitable
  place to store large arrays.
* `bytes` objects are supported natively by msgpack. If JSON is requested,
  conversion to unicode is attempted.
* `uuid.UUID` objects are converted into the standard hyphen-separated hex
  representation.
```

## Specs

Every node in Tiled has exactly one "structure family" ("node", "array",
"dataframe", etc.) It's useful to think of the structure family as a coarse,
lowest-common-denominator description of how to query and interpret the data.

Sometimes, it is useful to be more specific than the structure family.
Each Tiled node carries an optional list of "specs" (i.e. specifications).
These are meant to communicate that the metadata and/or data conforms to some
recognized layout, schema, or naming convention that may have meaning to
clients. Clients that recognize a spec can use it to provide a fine-tuned user
experience, such as more useful displays, specialized conveniences, and
performance optimizations.

Specs are given as a list of strings, meant to be ordered from most specific to
list specific. A spec may refer to a formally published specification or an
_ad hoc_ local convention. It is not necessary for every Tiled client to
understand every spec in use. A client can walk the list of specs in order and
stop if it finds a spec it recognizes. If a client does not recognize any of
the specs in the list, or if no specs are given, it can fall back to the
structure family to obtain a workable description of the data. Specs are just
an upgrade: "If you know what this means, you can use it to assume additional
constraints."

## References

Each Tiled structure carries an optional List of references, labeling links to
external resources. Their usage is open-ended. They may be used as links to:

* other structures or search results within this Tiled server or other Tiled servers
* data stored in other data storage systems unrelated to Tiled
* documents or publications related to the data

References have a label and a URL. Labels do not need to be unique. Example:

```py
references = [
    {"label": "raw_data", "url": "https://..."},
    {"label": "raw_data", "url": "https://..."},
    {"label': "doi", "url": "https://..."},
]
```

Each item must be a dict where the key is a string, and the value is a valid
URL. The formal definiton of URL is broad: it can include references to web
resources  (e.g.  `https://...`), local files (`file://localhost/...`), and
more.

In the future, we may expand this to integrate with richer standards for
describing relationships between data, such as RDF, OWL, XLink, and JSON-LD.
If you are interested in this, please open a GitHub issue describing your use case.
