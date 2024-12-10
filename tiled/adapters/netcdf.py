from pathlib import Path
from typing import Dict, List, Optional, Union

import xarray

from ..server.schemas import Asset
from ..structures.core import Spec
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import path_from_uri
from .protocols import AccessPolicy
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
    def from_assets(
        cls,
        assets: List[Asset],
        structure: Optional[
            TableStructure
        ] = None,  # NOTE: ContainerStructure? ArrayStructure?
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        **kwargs: Optional[Union[str, List[str], Dict[str, str]]],
    ) -> "NetCDFAdapter":
        filepath = path_from_uri(assets[0].data_uri)

        return read_netcdf(filepath)  # type: ignore
