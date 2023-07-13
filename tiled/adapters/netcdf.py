import xarray

from .xarray import DatasetAdapter


def read_netcdf(filepath):
    ds = xarray.open_dataset(filepath, decode_times=False)
    return DatasetAdapter.from_dataset(ds)
