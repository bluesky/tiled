# Metadata and "Specs"

## Metadata

Each Tiled node carries an optional dictionary of metadata. This is fully
under the control of the user: Tiled itself is not opinionated about its
content and does not reserve any names for its own use.

The metadata dictionary may be arbitrarily nested, and it may contain
anything that Tiled can transmit as JSON or [msgpack](https://msgpack.org).
All JSON-serializable types are supported:

- strings
- numbers
- objects (dictionaries)
- arrays (lists)

The following types are additionally supported:

- Dates, represented as `datetime.datetime` objects,
  are supported natively by msgpack. JSON does not support dates, so they are
  converted to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) strings
  when clients request JSON.
- Numpy objects are tolerated as input, but they are converted to (nested)
  lists before encoding to msgpack or JSON. Metadata is not a suitable
  place to store large arrays.
- `bytes` objects are supported natively by msgpack. If JSON is requested,
  conversion to unicode is attempted.
- `uuid.UUID` objects are converted into the standard hyphen-separated hex
  representation.

## Specs

Every Tiled node belongs to exactly one *structure family* (`container`, `array`,
`table`, etc.). It's useful to think of the structure family as a coarse,
lowest-common-denominator description of how to query and interpret the data.

Sometimes, it is useful to be more specific than the structure family.
Each Tiled node carries an optional list of "specs" (i.e. specifications).
These are meant to communicate that the metadata and/or data conforms to some
recognized layout, schema, or naming convention that may have meaning to
clients. Clients that recognize a spec can use it to provide a fine-tuned user
experience, such as more useful displays, specialized conveniences, and
performance optimizations.

A spec may refer to a formally published specification or an
_ad hoc_ local convention. It is not necessary for every Tiled client to
understand every spec in use. A client can walk the list of specs in order and
stop if it finds a spec it recognizes. If a client does not recognize any of
the specs in the list, or if no specs are given, it can fall back to the
structure family to obtain a workable description of the data. Specs are just
an upgrade: "If you know what this means, you can use it to assume additional
constraints."

Specs are given as a list, and by convention are ordered **from most to least specific**.
Depending on the circumstances, this ordering is resolved in two opposite directions:

- **Client recognition** walks the list front-to-back and uses the first (i.e. most specific) spec
  it recognizes, falling back to the structure family if it recognizes none. As a result,
  the most finely-tuned client is chosen.
- **Server validation** runs the validators back-to-front, i.e.
  least specific first, so that a broad spec can normalize the metadata
  before a more specific spec inspects it. See {doc}`../user-guide/validate-metadata` for
  how to declare which specs a server accepts and attach validators to them.

Each spec has a `name` (a string) and an optional `version` (also a string, or None).
