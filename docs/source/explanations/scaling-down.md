# Scaling Tiled Down

Tiled is a natural fit when you are operating the scale of a large scientific
facility with extensive data cataloging and access control systems.  However, if
you are operating as as an individual or a research group, Tiled scales down
nicely to provide a simple-to-configure solution without the overhead of
maintaining databases and other complex data systems. Tiled bridges these
domains nicely, giving a consistent data access experience.

Tiled's file-based mode of operation, i.e.

```
tiled serve directory ...
```

is designed to be an easy starting point and to serve situations where
traditional file-based workflows need to coexist with Tiled's service-based
ones.

## How Does a File Map to a Tiled "Structure"?

Tiled file-based tree tries to capture your logical understanding of what
your files on disk mean in a way that you can access programmatically and
consistently. Tiled exposes the _structure_ separately from how it happens
to be stored. A given data set---whether stored in CSV, Parquet, or a
spreadsheet in an Excel file---is of course the same data, and Tiled serves
it to the client in the same structure. If a collection data is stored in
heterogenous because of the way it was collected or due to other external
factors, those differences are hidden from the client. Likewise, if data
formats change over time (such as moving from CSV to a faster binary format) the
client-side code does not have to change.

In many situations, file maps to an obvious structure, one-to-one:

* CSV file -> dataframe
* TIFF image -> array
* Excel file -> collection of dataframes (one per spreadsheet)
* HDF5 file -> tree of arrays

When Tiled's file-based Tree walks the directory and discovers a file, it:

1. Identifies the file extension --- e.g. `".tif"` or `".tiff"`
2. Maps this to a media type (a.k.a. MIME type) --- e.g. `"image/tiff"`
3. Find a Tiled Adapter, whether built-in or user-provided, that is
   registered as a valid reader for that media type
4. On demand, uses that Tiled Adapter to extract the metadata and data from
   the file.
5. Serves the data to the client in the encoding of the client's choosing

Some structures span multiple files, such as:

* TIFF sequence -> an array where each individual file represents a slice
* Specialized formats like Zarr or TileDB that use a directory with an internal layout
* Multi-file HDF5 files
* *Ad hoc* formats consisting of a data file and a companion "metadata" file, such
  as a TIFF with a YAML that are logically associated via file names and should
  constitute a single "dataset"

To handle these, Tiled's file-based tree has a hook that passes control of the
entire directory over time a Tiled Adapter.

When Tiled's file-based Tree walks the directory and discovers a *subdirectory* it:

1. Passes the relative path to the subdirectory to the hook, `subdirectory_handler`,
   to see if this subdirectory has special internal structure.
2. If `subdirectory_handler` returns a Tiled Adapter, that Adapter is passed the path
   and left to its own devices to manage the files in that subdirectory.
3. If `subdirectory_handler` returns `None`, the file-based Tree proceeds to walk
   the contents of that subdirectory and treat any files therein as individuals as
   described above.
