# FAQ

**Is this something I run on my own machine, or a deployed service?**

Like Jupyter, it can be either. Jupyter users frequently launch ``jupyter
notebook`` on their own machines and use it "locally". Larger shared deployments
of Jupyter can be run on lab servers, facility clusters, or in the cloud.
Tiled is aiming for that same span of a use cases. It could be a tool for:

* Searching and loading data stored on our own machine
* Proxying and caching data stored in file-based systems on the web (e.g. Globus)
* Serving a public or private data repository for use by collaborators or the
  public

**What's the relationship to [TileDB](https://tiledb.com/) and [Zarr](https://zarr.readthedocs.io/en/stable/)?**

TileDB and Zarr are modern chunk-based storage formats that work on key--value
stores including traditional file systems and blob (e.g. S3) storage. They have
many virtues that Tiled does not, and if it is practical for you to transcode your
data into TileDB or Zarr, you should take a close look at that solutions. Tiled
aims to be *agnostic* about storage formats and offer best-effort chunk-based
access and search capabilities on top of various databases and both
traditional and modern formats. Tiled is a web service, not a file format.

**What's the relationship to [Intake](https://intake.readthedocs.org/)**?

See {doc}`lineage` for background.

**What is the future plans for Databroker?**

See {doc}`lineage` for background. Databroker will likely be refactored to
extend Tiled, adding specifics related to Bluesky's Document Model and
storage backends.

**Can I upload data to Tiled?**

Yes, this was recently added. See {doc}`../tutorials/writing`.
