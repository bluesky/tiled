from pathlib import Path
from typing import List, Union

import xarray

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
