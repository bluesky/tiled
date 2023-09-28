import importlib

from ..serialization.table import XLSX_MIME_TYPE
from ..utils import OneShotCachedMap

# This maps MIME types (i.e. file formats) for appropriate Readers.
# OneShotCachedMap is used to defer imports. We don't want to pay up front
# for importing Readers that we will not actually use.
PARQUET_MIMETYPE = "application/x-parquet"
SPARSE_BLOCKS_PARQUET_MIMETYPE = "application/x-parquet-sparse"  # HACK!
ZIP_MIMETYPE = "application/zip"
ZARR_MIMETYPE = "application/x-zarr"
DEFAULT_ADAPTERS_BY_MIMETYPE = OneShotCachedMap(
    {
        "image/tiff": lambda: importlib.import_module(
            "...adapters.tiff", __name__
        ).TiffAdapter,
        "multipart/related;type=image/tiff": lambda: importlib.import_module(
            "...adapters.tiff", __name__
        ).TiffSequenceAdapter.from_files,
        "text/csv": lambda: importlib.import_module(
            "...adapters.csv", __name__
        ).read_csv,
        XLSX_MIME_TYPE: lambda: importlib.import_module(
            "...adapters.excel", __name__
        ).ExcelAdapter.from_file,
        "application/x-hdf5": lambda: importlib.import_module(
            "...adapters.hdf5", __name__
        ).HDF5Adapter.from_file,
        "application/x-netcdf": lambda: importlib.import_module(
            "...adapters.netcdf", __name__
        ).read_netcdf,
        PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.parquet", __name__
        ).ParquetDatasetAdapter,
        SPARSE_BLOCKS_PARQUET_MIMETYPE: lambda: importlib.import_module(
            "...adapters.sparse_blocks_parquet", __name__
        ).SparseBlocksParquetAdapter,
        ZARR_MIMETYPE: lambda: importlib.import_module(
            "...adapters.zarr", __name__
        ).read_zarr,
        ZIP_MIMETYPE: lambda: importlib.import_module(
            "...adapters.awkward_buffers", __name__
        ).AwkwardBuffersAdapter.from_directory,
    }
)

# We can mostly rely on mimetypes.types_map for the common ones
# ('.csv' -> 'text/csv', etc.) but we supplement here for some
# of the more exotic ones that not all platforms know about.
DEFAULT_MIMETYPES_BY_FILE_EXT = {
    # This is the "official" file extension.
    ".h5": "application/x-hdf5",
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
