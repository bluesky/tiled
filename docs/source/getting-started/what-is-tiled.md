# What is Tiled?

Tiled provides an API to multimodal scientific data.  It supports N-dimensional
array data, tabular data, and nested structures containing these. It also
supports specialized array types, including sparse arrays and [awkward][]
arrays.

Tiled transport data securely via (parallel) download and upload over HTTP.
But can also be used as a catalog, pointing to data assets that are then
accessed directly by other means.

Tiled enables:
- **Search** on metadata
- Parallel **download** of raw assets
-

Tiled was developed with Scientific User Facilities, and it works well with
[Bluesky], but it was designed from the start as a **general-purpose** API to
scientific data structures.

[awkward]: https://awkward-array.org
[Bluesky]: https://blueskyproject.org/bluesky
