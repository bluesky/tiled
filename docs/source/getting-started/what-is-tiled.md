# What is Tiled?

Tiled helps scientists to store, find, and access scientific data at scale.
It provides a _structured_ API to scientific data, featuring remote
slicing and data format conversion of multimodal datasets. Tiled can run on a
laptop as a single process, or it can be deployed at scale in a data center.

Scientists can download or upload data over HTTP, with support for
parallel transfers. Tiled can also serve as a catalog, pointing to data assets
that are stored and accessed through other systems.

Tiled enables scientists to:
- **Search** on metadata
- **Slice** remotely into arrays and tables, downloading only what they need
- **Transcode** between formats, letting the requester specify how they want
  the data
- **Locate** or **download** underlying data files or blobs
- **Stream** live or recent data
- **Upload** or **register** datasets to add them to Tiled

Tiled includes a Python client for reading and writing data, with strong
integration with libraries like numpy and pandas. Data can also be accessed
through a browser-based web interface or from any program that speaks HTTP,
including command-line tools like [curl][] and [HTTPie][]. Users have built custom
web and desktop applications on top of Tiled, as well as integrations with
scientific tools like [Igor][].

Tiled supports N-dimensional array data, tabular data, and nested
directory-like structures containing these. It also supports specialized array
types, including sparse arrays and [awkward][] arrays. It was developed to be
the recommended data store for raw data from [Bluesky][], but was designed from
the start to be more general: suitable for any raw, processed, or analyzed
scientific data.

Tiled uses web security standards, so it works with existing institutional
infrastructure out of the box. Access can be configured with fine-grained
policies tied to an institution's authentication and authorization systems,
or it can be run as a simple single-user server with no setup. The server's
security has been vetted by third-party penetration testers.

[awkward]: https://awkward-array.org
[Bluesky]: https://blueskyproject.org/bluesky
[curl]: https://curl.se/
[HTTPie]: https://httpie.io/
[Igor]: https://www.wavemetrics.com/
