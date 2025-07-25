# How Tiled Fits into the Ecosystem

## Design Principles

Many institutions are building tools for structured data access.
Tiled's particular approach to this problem is tuned to the needs of its users.
Our users are opinionated scientists with highly varied data scales, access
patterns, and conventions. From their requirements flow Tiled's design.

* Be unopinionated about the format(s) data is stored in or accessed in.
  There will be never "one format to rule them all" across all scientific
  domains of interest to Tiled. Standardize on common _structures_ not
  _formats_. Transcode data upon request into the format most convenient to
  the user.
* Meet all users where they are. Support both traditional data storage
  (directories of files with metadata in filenames) and forward-looking ones
  involving databases. Provide a smooth path to enable users to explore more
  powerful data storage and access solutions without disrupting their existing
  workflows.
* Use web technologies and standards to make data available to users in a way
  that is secure and easily interoperable with both legacy and emerging data
  consumers. These include data science tools, web portals, and traditional
  file-based software.

## Example Use Cases

* I want to access and share URLs that point to specific slices of my data.
* I want to keep messy, brittle I/O code out of my data analysis code. I want a Python
  API (and maybe someday APIs in other languages, like
  [Julia](https://julialang.org/)) that gives me numpy/pandas/xarray/whatever
  directly.
* I have files in Format A but my data analysis program needs than in
  Format B.
* I have files in Format A and I write my data analysis program to
  work against Format A. Later I want to {move, rename, switch to Format B, move
  from file-based storage to blob storage} and I don't want to break my code. I
  rely on Tiled to provide it the same as always.
* Some of my users just want files; other users want to work in numpy and not
  think about files. The second group can use Tiled sitting on top of those files,
  and the first group can continue as they were.
* I want to transition from a file-based workflow to a
  database-backed workflow some day, and I don't want all my analysis scripts to
  break.

## What Tiled is Not

* Tiled is **not a file-based data access service** like
  [Globus](https://www.globus.org/). File-based servers do not "see inside"
  the files and therefore cannot generally support semantic partial access,
  as in "Download columns A and C only" or "Download the middle frame of this
  image time series so I can see if it's the one I'm interested in."
* Tiled is a **not a storage format** like [TileDB](https://tiledb.com/) or
  [Zarr](https://zarr.readthedocs.io/en/stable/). It is expected to
  work well with those formats because they enable efficient chunk-based access,
  but it is designed to work with less modern formats as well, to accept the
  data "as is" and convert it on the fly upon request.
* Tiled is **not a data analysis and visualization platform** like
  [Plotly Chart Studio](https://chart-studio.plotly.com/),
  but it can be used to serve data into them.
* Tiled is **not an application server** like  [Vaex](https://vaex.io/). Tiled
  offers no compute; it is focused only on I/O.
* Tiled is a **not a server for specific data format** like
  [Xpublish](https://xpublish.readthedocs.io/) or
  [JupyterLab HDF5](https://github.com/jupyterlab/jupyterlab-hdf5), though
  it may be possible to build similar things with Tiled easily these formats
  and others.

## Lineage

The originating authors of Tiled have worked on several previous, related
projects.

* [**PIMS**](https://soft-matter.github.io/pims/) (2013, JHU Leheny Lab), or Python
  Image Sequence, abstracts over a variety of image time series formats with a
  lazy-loading, numpy-slicable Python object. It has some goals in common with
  [**Dask Array**](https://docs.dask.org/en/latest/array.html) but it is tightly
  focused on image time series, which lends it
  [ceratin advantages](https://github.com/danielballan/pims2-prototype/issues/1#issuecomment-595653031)
  for that use case. PIMS is still maintained and widely used, including as a
  dependency of [dask-image](https://github.com/dask/dask-image).
* [**Databroker**](https://blueskyproject.io/databroker)
  (2015, Brookhaven National Laboratory) provides searchable dict-like
  "catalogs" of data with entries that ultimately provide N-dimensional or
  tablular data as PIMS objects or [pandas](https://pandas.pydata.org/)
  DataFrames respectively. It emphasizes support for the Bluesky "Document Model"
  for streaming data.
* [**Intake**](https://intake.readthedocs.org/) (2017, Anaconda Inc.) is similar
  to Databroker in its goals and feature set---searchable catalogs of data that
  ultimately provide standard SciPy data structures---but its intended scope
  is broader than Databroker's, not being tied to the Bluesky "Document
  Model" in particular. In 2018--2020, Databroker was refactored to become a set
  of Intake drivers.

Intake also has a prototype of an HTTP server, which has not yet been fully
developed into a robust tool. Attempts to rework that server led to the
conclusion that Intake's architecture and API privilege ergonomic *interactive*
use and direct access from Python. The changes necessary to use it as
library code effectively within a performant service or larger application
would be largely *subtractive*, in tension with its use as a user-facing
interactive exploratory tool.

Which brought us to Tiled (2021, Brookhaven National Laboratory with the Bluesky
Collaboration). Tiled is designed with HTTP-based access as the driving use
case, targeting Python clients, browser-based clients, and clients in other
languages. Tiled is compatible with PIMS for reading image series. Tiled has
search capability and some other features drawn from Databroker. Tiled has a
*node* abstraction similar to Intake's *Catalog*, with small but important
differences to suit the server--client interaction. (It is hoped that Tiled
Nodes and Intake Catalogs can be made interoperable in the near future.)
Finally, Tiled has an *Adapter* abstraction that is something like an amalgam of
PIMS Readers and Intake's DataSources.

In summary:

* Tiled can use PIMS internally to efficiently slice and read image series.
* Tiled can probably be made interoperate with Intake objects.
* Databroker will evolve to be a thin wrapper around Tiled, adding
  concepts and capabilities specific to the Bluesky "Document Model".
