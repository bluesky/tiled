"""Pure, I/O-free protocol knowledge shared by the Tiled client shells.

The modules in this package hold the client's protocol knowledge -- request
builders, response parsers, and failure classifiers -- as pure functions and
dataclasses with no I/O. The client classes ("shells") turn request drafts
into actual HTTP requests, own retries/throttling/progress reporting, and
interpret responses with the matching parser functions. Keeping this layer
pure makes it directly unit-testable and shareable between synchronous and
asynchronous client implementations. Nothing in this package touches httpx
types: drafts are plain data (strings, dicts, bytes), which keeps the
protocol independent of the HTTP library.

Three levels of description are used, by convention:

- A `RequestDraft` describes exactly one HTTP request, fully determined and
  pre-encoded, including its retry policy (`RetryPolicy`). Builders named
  `draft_request_<verb>_<family>_<endpoint>`
  (e.g. `draft_request_write_array_block`) return one; builders that live as
  methods on a class carrying their shared parameters drop only the
  `<family>` part -- the class supplies the family, and the name keeps the
  verb (e.g. `ChildrenSearchBuilder.draft_request_read_page`).
  Operations with nothing to decode and no short-circuit need nothing more
  (e.g.typical write operations).
  There is exactly one draft class: family semantics live in the builder modules,
  shared wire mechanics live on `RequestDraft` itself (query folding, `get_or_post`),
  and nothing downstream dispatches on a draft's family -- that uniformity is what
  allows a single executor per shell (`send_request_draft`).
- A `*Builder` dataclass groups the draft definitions for several related
  operations against one resource, holding parameters computed once per
  resource (e.g. `PaginatedCollectionBuilder` for one paginated collection
  endpoint, specialized by `ChildrenSearchBuilder` for a container's
  searched/sorted children). Unlike a recipe, a builder family describes no
  single operation: each draft it produces stands alone.
- A `*Recipe` dataclass (e.g. `ArrayReadRecipe`, `CreateNodeRecipe`)
  describes one logical operation as a whole: the draft(s) of request(s) to
  send -- keyed by their place in the operation -- plus the pure values
  needed to interpret the responses (e.g. `dtype` and chunk tiling for
  decoding and assembly) or to carry the operation across attempts (the
  create flow's adoption guard and failure classification). Classmethod
  constructors named `for_*` (after the endpoint or operation they
  describe) build one; the shells carry it out. A recipe whose decode
  parameters live wholly on itself exposes a `parse(content)` method
  (sparse, awkward, ragged); the array decode is a `parse` staticmethod
  taking the decode parameters as arguments, so dask tasks can carry
  per-draft primitives and reference it by qualified name without
  embedding the recipe. A recipe holds plain `RequestDraft`s directly
  (possibly with no entry at all where the result can be constructed
  without a request); recipes are never nested.

Module map: `request.py` (the draft, retry policy, link derivation, generic
GET/DELETE builders), `search.py` (paginated collections, query encoding,
children search), `node.py` (node fetch/metadata mutation, create,
assets), `dataset.py` (pure planning for assembling xarray datasets from
a node's children), and one module per structure family (`array.py`,
`table.py`, `sparse.py`, `awkward.py`, `ragged.py`).
"""
