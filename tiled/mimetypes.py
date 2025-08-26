import copy
import importlib

from .serialization.table import XLSX_MIME_TYPE
from .utils import APACHE_ARROW_FILE_MIME_TYPE, OneShotCachedMap

# This maps MIME types (i.e. file formats) for appropriate Readers.
# OneShotCachedMap is used to defer imports. We don't want to pay up front
# for importing Readers that we will not actually use.
PARQUET_MIMETYPE = "application/x-parquet"
SPARSE_BLOCKS_PARQUET_MIMETYPE = "application/x-parquet;structure=sparse"
ZARR_MIMETYPE = "application/x-zarr"
AWKWARD_BUFFERS_MIMETYPE = "application/x-awkward-buffers"
TILED_SQL_TABLE_MIMETYPE = "application/x-tiled-sql-table"
# TODO: make type[Adapter] after #1047
DEFAULT_ADAPTERS_BY_MIMETYPE = OneShotCachedMap[str, type](
    {
        "image/tiff": lambda: importlib.import_module(
            "..adapters.tiff", __name__
        ).TiffAdapter,
        "multipart/related;type=image/tiff": lambda: importlib.import_module(
            "..adapters.tiff", __name__
        ).TiffSequenceAdapter,
        "image/jpeg": lambda: importlib.import_module(
            "..adapters.jpeg", __name__
        ).JPEGAdapter,
        "multipart/related;type=image/jpeg": lambda: importlib.import_module(
            "..adapters.jpeg", __name__
        ).JPEGSequenceAdapter,
        "text/csv": lambda: importlib.import_module(
            "..adapters.csv", __name__
        ).CSVAdapter,
        "multipart/related;type=text/csv": lambda: importlib.import_module(
            "..adapters.csv", __name__
        ).CSVAdapter,
        # https://www.rfc-editor.org/rfc/rfc4180#section-3
        "text/csv;header=present": lambda: importlib.import_module(
            "..adapters.csv", __name__
        ).CSVAdapter,
        "text/csv;header=absent": lambda: importlib.import_module(
            "..adapters.csv", __name__
        ).CSVArrayAdapter,
        XLSX_MIME_TYPE: lambda: importlib.import_module(
            "..adapters.excel", __name__
        ).ExcelAdapter,
        "application/x-hdf5": lambda: importlib.import_module(
            "..adapters.hdf5", __name__
        ).HDF5Adapter,
        "application/x-netcdf": lambda: importlib.import_module(
            "..adapters.netcdf", __name__
        ).NetCDFAdapter,
        "application/x-npy": lambda: importlib.import_module(
            "..adapters.npy", __name__
        ).NPYAdapter,
        "multipart/related;type=application/x-npy": lambda: importlib.import_module(
            "..adapters.npy", __name__
        ).NPYSequenceAdapter,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "..adapters.parquet", __name__
        ).ParquetDatasetAdapter,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "..adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter,
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "..adapters.zarr", __name__
        ).ZarrAdapter,
        AWKWARD_BUFFERS_MIMETYPE: lambda: importlib.import_module(
            "..adapters.awkward_buffers", __name__
        ).AwkwardBuffersAdapter,
        APACHE_ARROW_FILE_MIME_TYPE: lambda: importlib.import_module(
            "..adapters.arrow", __name__
        ).ArrowAdapter,
        TILED_SQL_TABLE_MIMETYPE: lambda: importlib.import_module(
            "..adapters.sql", __name__
        ).SQLAdapter,
    }
)

DEFAULT_REGISTRATION_ADAPTERS_BY_MIMETYPE = copy.deepcopy(DEFAULT_ADAPTERS_BY_MIMETYPE)

DEFAULT_REGISTRATION_ADAPTERS_BY_MIMETYPE.set(
    "text/csv",
    lambda: importlib.import_module("..adapters.csv", __name__).CSVAdapter,
)


# We can mostly rely on mimetypes.types_map for the common ones
# ('.csv' -> 'text/csv', etc.) but we supplement here for some
# of the more exotic ones that not all platforms know about.
DEFAULT_MIMETYPES_BY_FILE_EXT = {
    # This is the "official" file extension.
    ".h5": "application/x-hdf5",
    # This is the Numpy on-disk format.
    ".npy": "application/x-npy",
    # This is NeXus. We may want to invent a special media type
    # like 'application/x-nexus' for this, but I'll punt that for now.
    # Needs thought about how to encode the various types of NeXus
    # (media type arguments, for example).
    ".nxs": "application/x-hdf5",
    # These are unofficial but common file extensions.
    ".hdf": "application/x-hdf5",
    ".hdf5": "application/x-hdf5",
    # on opensuse csv -> text/x-comma-separated-values
    ".csv": "text/csv",
    ".zarr": ZARR_MIMETYPE,
}
