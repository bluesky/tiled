from collections.abc import Sequence
from pathlib import Path
from typing import Any

import xarray

from tiled.adapters.table import TableAdapter

from ..catalog.orm import Node
from ..structures.data_source import DataSource
from ..utils import path_from_uri
from .xarray import DatasetAdapter


def read_netcdf(filepath: str | list[str] | Path) -> DatasetAdapter:
    """

    Parameters
    ----------
    filepath :

    Returns
    -------

    """
    ds = xarray.open_dataset(filepath, decode_times=False)
    return DatasetAdapter.from_dataset(ds)


class NetCDFAdapter(TableAdapter):
    # The catalog passes the directories of its readable storage to
    # from_catalog as `readable_directories`.
    accepts_readable_directories = True

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[None],
        node: Node,
        /,
        readable_directories: Sequence[str | Path] | None = None,
        **kwargs: Any | None,
    ) -> "NetCDFAdapter":
        filepath = path_from_uri(data_source.assets[0].data_uri)
        if readable_directories is not None:
            # A NetCDF-4 file is an HDF5 file, which can make the library read
            # other files. A server reads only the file that was registered.
            from .hdf5 import require_self_contained

            require_self_contained(filepath)

        return read_netcdf(filepath)  # type: ignore
