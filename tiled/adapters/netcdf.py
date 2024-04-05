from pathlib import Path
from typing import Union

import xarray

from .xarray import DatasetAdapter


def read_netcdf(filepath: Union[str, list[str], Path]) -> DatasetAdapter:
    ds = xarray.open_dataset(filepath, decode_times=False)
    return DatasetAdapter.from_dataset(ds)
