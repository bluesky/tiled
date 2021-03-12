# Lineage

The originating authors of Tiled have worked on several previous, related
projects.

* [**PIMS**](https://soft-matter.github.io/pims/) (2013, JHU Leheny Lab), or Python
  Image Sequence, abstracts over a variety of image time series formats with a
  lazy-loading, numpy-slicable Python object. It has some goals in common with
  [**Dask Array**](https://docs.dask.org/en/latest/array.html) but it is tightly
  focused on image time series, which lends it
  [ceratin advantages](https://docs.dask.org/en/latest/array.html) for that use
  case. PIMS is still maintained and widely used, including as a dependency of
  [dask-image](https://github.com/dask/dask-image).
* [**Databroker**](https://blueskyproject.io/databroker)
  (2015, Brookhaven National Laboratory) provides searchable dict-like
  "catalogs" of data with entries that ultimately provide N-dimensional or
  tablular data as PIMS objects or [pandas](https://pandas.pydata.org/)
  DataFrames respectively. It also provides streaming access to data and is
  tightly coupled to the Bluesky "Document Model" for streaming data.
* [**Intake**](https://intake.readthedocs.org/) (2017, Anaconda Inc.) is similar
  Databroker in its goals and feature set---searchable Catalogs that ultimately
  provide standard SciPy data structures---but its intended scope is broader
  than Databroker's, not being tied to the Bluesky "Document Model" in
  particular. It also supports *nested* Catalogs and chainable search queries.
  In 2019--2020, Databroker was refactored to become an Intake plugin.

Intake also has a prototype of an HTTP server, which has not yet been fully
developed into a robust tool. Attempts to rework that server led to the
conclusion that Intake's architecture and API privilege ergonomic *interactive*
use, and the changes neccessary to use it as library code effectively within a
performant server (or other larger applicaiton) would be largely *subtractive*,
in tension with its use as a user-facing interactive exploratory tool.

Which brings us to Tiled (2021, Brookhaven National Laboratory with the Bluesky
Collaboration). Tiled is designed with HTTP-based access as the driving use
case, targeting Python clients, browser-based clients, and clients in other
languages. Tiled is compatible with PIMS for reading image series. Tiled has
search capability and some other features drawn from Databroker. Tiled has a
*Catalog* abstraction similar to Intake's, with small but important differences
to suit the server--client interaction. (It is hoped that Tiled Catalogs and
Intake Catalogs can be made interoperable in the near future.) Finally, Tiled
has a *Reader* abstraction that is something like an amalgam of PIMS Readers and
Intake's Datasources.
