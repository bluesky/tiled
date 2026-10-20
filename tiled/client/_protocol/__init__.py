"""Pure, I/O-free protocol knowledge shared by the Tiled client shells.

The modules in this package hold the client's protocol knowledge -- request
builders, response parsers, and failure classifiers -- as pure functions and
dataclasses with no I/O. The client classes ("shells") turn request drafts
into actual HTTP requests, own retries/throttling/progress reporting, and
interpret responses with the matching parser functions. Keeping this layer
pure makes it directly unit-testable and shareable between synchronous and
asynchronous client implementations.

Two levels of description are used, by convention:

- A `RequestDraft` describes exactly one HTTP request, fully determined and
  pre-encoded. Builders named `draft_*` return one; operations with nothing
  to decode and no short-circuit (typically writes) need nothing more.
  There is exactly one draft class: family semantics live in the builder
  modules, shared wire mechanics live on `RequestDraft` itself (query
  folding, `get_or_post`), and nothing downstream dispatches on a draft's
  family -- that uniformity is what allows a single executor per shell.
- A `*Recipe` dataclass (e.g. `ArrayReadRecipe`) describes one logical
  operation as a whole: the draft(s) of request(s) to send -- keyed by their
  place in the operation -- plus the pure context needed to interpret the
  responses (e.g. `dtype` and chunk tiling for decoding and assembly).
  Classmethod constructors named `for_*` (after the endpoint or operation
  they describe) build one; the shells carry it out. A recipe holds plain
  `RequestDraft`s directly (possibly with no entry at all where the result
  can be constructed without a request); recipes are never nested.
"""
