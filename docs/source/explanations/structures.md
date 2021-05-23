# Structures

Tiled *Readers* provide data in one of a fixed group of standard *structure families*.
These are *not* Python-specific structures. They can be encoded in standard,
language-agnostic formats and transferred from the service to a client in
potentially any language.

## Supported structure families

Five structure families are currently supported. The most widespread are:

* array --- a strided array, like a numpy array
* dataframe --- a table with column labels an index(es), as in Apache Arrow or pandas

These additional three structures come from
[xarray](https://xarray.pydata.org/en/stable/). They may be considered
*containers* for one or more strided arrays, grouped together and marked up with
some additional metadata, such as labeled dimensions.

* variable --- one strided array with some additional metadata
* data_array --- one or more strided arrays (the extras are "coordinates")
* dataset --- a group of strided arrays with shared coordinates

Support [Awkward Array](https://awkward-array.org/) is planned.

Adding support for a new structure is one of the few things in Tiled that is
*not* "pluggable" or extensible by downstream code. It requires a deep change in
the server and touches several aspects of the library.

## How structure is encoded

The structures are designed to be as unoriginal as possible, using standard
names from numpy, pandas/Arrow, and xarray.

The structures are encoded in two parts:

* **Macrostructure** --- This is the high-level structure including things like
  shape, chunk shape, number of partitions, and column names. This structure
  *has meaning to the server* and shows up in the HTTP API.
* **Microstructure** --- This is low-level structure including things like
  machine data type(s) and partition boundary locations. It enables the
  service-side reader to communicate to the client how to decode the bytes.

## Examples

TO DO: Show example JSON here.