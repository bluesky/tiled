# What is Tiled?

Tiled helps scientists to store, find, and access scientific data at scale. It
provides a _structured_ API to scientific data structures of varied types so
users can slice, convert, and retrieve just what they need. Tiled is designed
to run on a scientist's laptop or deploy at scale in a facility's data center.

Users can download or upload data over HTTP, with support for
parallel transfers. Tiled can also serve as a catalog, pointing to data assets
that are stored and accessed through other systems.

Tiled enables scientists to:
- **Search** across metadata
- **Slice** remotely into arrays and tables
- **Transcode** data into the format they need
- **Locate** or **download** underlying data files or blobs
- **Stream** live data and "replay" recent data
- **Upload** or **register** datasets to add them to Tiled

Tiled includes a Python client for reading and writing data, with strong
integration with libraries like numpy, pandas, and dask. Data can also be
accessed through a browser-based web interface or from any HTTP client,
including command-line tools like [curl][] and [HTTPie][]. Users have built
custom web and desktop applications on Tiled, as well as integrations with
scientific tools like [Igor][].

Data structures are first-class citizens in Tiled: it supports N-dimensional
array data, tabular data, and nested directory-like containers of these. Tiled
also supports specialized array types, including sparse arrays and [awkward][]
arrays. It was built to be the recommended data store for [Bluesky][] _and_ to
be a general-purpose store for any raw, processed, or analyzed scientific data.
It is used in the wild as both.

Tiled uses web security standards, so it works with existing institutional
infrastructure. Access can be configured with fine-grained policies tied to an
institution's authentication and authorization systems, or it can be run as a
simple single-user server with no setup. The server's security is regularly
vetted by third-party penetration testers.

[awkward]: https://awkward-array.org
[Bluesky]: https://blueskyproject.org/bluesky
[curl]: https://curl.se/
[HTTPie]: https://httpie.io/
[Igor]: https://www.wavemetrics.com/
