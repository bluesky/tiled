import dask.dataframe

from ..server.object_cache import NO_CACHE, get_object_cache
from .dataframe import DataFrameAdapter


def read_csv(
    *args,
    structure=None,
    metadata=None,
    specs=None,
    access_policy=None,
    **kwargs,
):
    """
    Read a CSV.

    Internally, this uses dask.dataframe.read_csv.
    It forward all parameters to that function. See
    https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.read_csv

    Examples
    --------

    >>> read_csv("myfiles.*.csv")
    >>> read_csv("s3://bucket/myfiles.*.csv")
    """
    ddf = dask.dataframe.read_csv(*args, **kwargs)
    # If an instance has previously been created using the same parameters,
    # then we are here because the caller wants a *fresh* view on this data.
    # Therefore, we should clear any cached data.
    cache = get_object_cache()
    if cache is not NO_CACHE:
        cache.discard_dask(ddf.__dask_keys__())
    # TODO Pass structure through rather than just re-creating it
    # in from_dask_dataframe.
    return DataFrameAdapter.from_dask_dataframe(
        ddf, metadata=metadata, specs=specs, access_policy=access_policy
    )


read_csv.__doc__ = (
    """
This wraps dask.dataframe.read_csv. Original docstring:

"""
    + dask.dataframe.read_csv.__doc__
)
