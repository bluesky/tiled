from pathlib import Path
from typing import Any, List, Optional, Union

import xarray

from ..catalog.orm import Node
from ..structures.data_source import DataSource
from ..utils import path_from_uri
from .xarray import DatasetAdapter


def read_netcdf(filepath: Union[str, List[str], Path]) -> DatasetAdapter:
    """

    Parameters
    ----------
    filepath :

    Returns
    -------

    """
    ds = xarray.open_dataset(filepath, decode_times=False)
    return DatasetAdapter.from_dataset(ds)


class NetCDFAdapter:
    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[None],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "NetCDFAdapter":
        filepath = path_from_uri(data_source.assets[0].data_uri)

        return read_netcdf(filepath)  # type: ignore
