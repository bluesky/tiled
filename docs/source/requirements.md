# Requirements

*This document is slightly out of date, as the requirements have evolved
in tandem with the exploratory implementation.*

* HTTP API that supports JSON and msgpack requests, with JSON and msgpack
  responses, as well as binary blob responses for chunked data
* Be usable from any HTTP client and from languages other that Python. Avoid
  baking any Python-isms deeply into it. No pickle, no msgpack-python. (Those
  can be *options*, for the purpose of communnicating with a Python client, but
  they should be priviledged or default.)
* Effeciently list and access entries, with pagation and random access.
* Efficiently search entries using an extensible text of queries including broad
  ones like "full text search" and more application- and dataset-specific
  queries.
* Access metadata cheaply.
* Serve data from numpy arrays, DataFrames, and xarray structures in various
  formats in a tile-based (chunked, partitioned) fashion.
* A Python client with rich proxy objects that do chunk-based access
  transparently (like Intake's `RemoteXarray` and similar). But, differently
  from current Intake and Databroker, do not switch dask-vs-not-dask or
  dask-vs-another-delayed-framework at call time. Use a consistent delayed
  framework (or none at all) consistently within a given context. Your only
  option at call time should be `read()`. Whether that is in memory, dask, or
  something else should be set higher up---for example, on the client instance.
* Usable performance without any *intrinsic* caching in the server. Objects may
  do some internal caching for optimization, but the server will not
  explicitly hang on to any state between requests.
* Path toward adding state / caching in external systems (e.g. Redis, nginx)


