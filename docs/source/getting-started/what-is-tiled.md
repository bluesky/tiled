# What is Tiled?

Tiled provides an API to multimodal scientific data. It supports N-dimensional
array data, tabular data, and nested directory-like structures containing
these. It also supports specialized array types, including sparse arrays and
[awkward][] arrays.

Tiled transports data securely via (parallel) download and upload over HTTP.
It can also be used just as a catalog, pointing to data assets that are
accessed directly by some other means.

Tiled enables scientists to:
- **Search** on metadata
- **Slice** remotely into arrays and tables, downloading only what they need
- **Transcode** between formats (The storage format is not always
  accessible to the program that wants the data!)
- **Locate** or **download** underlying data files or blobs
- **Stream** live or recent data (via websockets)
- **Upload** or **register** datasets to add them to Tiled

Tiled includes:
- a **webserver**
- a Python-based **client** that integrates well with scientific Python
  libraries (e.g. numpy, pandas) and **AI tools**
- and a prototype **web app** for browser-based data access

Tiled implements web security standards, which makes it easy to integrate
with IT infrastructure without special arrangement or exceptions. Its security
has been vetted by third-party penetration testers. It can integrate with
authentication and authorization systems to implement fine-grained access
policies. Alternatively, for simple setups, it can be deployed as a single-user
server.

Tiled was designed from the start as a **general-purpose** API to scientific
data structures. Tiled works well with [Bluesky], but the word "bluesky" is
nowhere in the Tiled codebase; it can accept arbitrary array or tabular data.

[awkward]: https://awkward-array.org
[Bluesky]: https://blueskyproject.org/bluesky
