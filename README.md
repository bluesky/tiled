# Tiled

Tiled is a service that enables **secure, structured** access to **scientific
data**. It supports **search**, remote **slicing**, **format** conversion, and
live **streaming**.

Tiled helps scientists to store, find, and access scientific data at scale. It
provides a API that understands scientific data structures of varied types so
users can slice, convert, and retrieve just what they need. Tiled is designed
to run on a scientist's laptop or deploy at scale in a facility's data center.

Tiled is a fully [open source][] project developed under a multi-institutional
[governance model][].

Tiled enables scientists to:
- **Search** across metadata
- **Slice** remotely into arrays and tables
- **Transcode** data into the format they need
- **Locate** or **download** underlying data files or blobs (e.g. S3 buckets)
- **Stream** live data and "replay" recent data
- **Upload** or **register** datasets to add them to Tiled

## Installation

With **pip** or **uv**, install `"tiled[all]"` or just `"tiled[client]"` if you only need
to access data (not run your own Tiled server).

With **conda** or **pixi**, install `tiled` or just `tiled-client`. Tiled is available
on the `conda-forge` channel, which pixi uses by default.

See [Getting Started][] for more detailed installation instructions.


## First Steps

See [What is Tiled?][] for an overview of Tiled's goals. Then see
[10 Minutes to Tiled][] for a walkthrough of some key features.

[open source]: https://github.com/bluesky/tiled/blob/main/LICENSE
[governance model]: https://github.com/bluesky/governance
[Getting Started]: https://blueskyproject.io/tiled/getting-started/index.html
[What is Tiled?]: https://blueskyproject.io/tiled/getting-started/what-is-tiled.html
[10 Minutes to Tiled]: https://blueskyproject.io/tiled/getting-started/10-minutes-to-tiled.html
