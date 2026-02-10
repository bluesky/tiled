# What is Tiled?

Tiled provides a structured API to multimodal scientific data. It is a
universal translator designed to help scientists to store, find, and access
scientific data at scale.

It can transport data securely via (parallel) download and upload over HTTP.
Alternatively, it can be used just as a catalog, pointing to data assets that
are accessed directly by some other means.

Tiled enables scientists to:
- **Search** on metadata
- **Slice** remotely into arrays and tables, downloading only what they need
- **Transcode** between formats, letting the requester specify how they want
  the data
- **Locate** or **download** underlying data files or blobs
- **Stream** live or recent data (via websockets)
- **Upload** or **register** datasets to add them to Tiled

The software package includes:
- a **webserver**
- a **Python-based client** for reading and writing data, which integrates well
  with scientific Python libraries (e.g., numpy, pandas)
- and a prototype **web app** for browser-based data access

Tiled's use of web standards makes it easy to access from any program
that speaks HTTP. Users have written custom web and desktop apps as well as
integrations with scientific applications like [Igor][]. Tiled can
be used from command-line HTTP tools like [curl][] or [HTTPie][].

Tiled was designed from the start to be a **general-purpose** API for scientific
data structures. It supports N-dimensional array data, tabular data, and nested
directory-like structures containing these. It also supports specialized array
types, including sparse arrays and [awkward][] arrays. It integrates well with
[Bluesky][].

The server implements web security standards, which makes it easy to integrate
with IT infrastructure without special arrangement or exceptions. Its security
has been vetted by third-party penetration testers. It can integrate with
authentication and authorization systems to implement fine-grained access
policies. Alternatively, for simple setups, it can be deployed as a single-user
server.

[awkward]: https://awkward-array.org
[Bluesky]: https://blueskyproject.org/bluesky
[curl]: https://curl.se/
[HTTPie]: https://httpie.io/
[Igor]: https://www.wavemetrics.com/
