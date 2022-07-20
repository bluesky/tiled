from ..utils import modules_available


def register_builtin_serializers():
    """
    Register built-in serializers for each structure if its associated module(s) are installed.
    """
    # Each submodule in ..serialization registers serializers on import.
    # Some are conditional on the availability of particular I/O libraries.
    from ..serialization import node as _node  # noqa: F401

    del _node
    if modules_available("numpy", "dask.array"):
        from ..serialization import array as _array  # noqa: F401

        del _array
    if modules_available("pandas", "pyarrow", "dask.dataframe"):
        from ..serialization import dataframe as _dataframe  # noqa: F401

        del _dataframe
    if modules_available("sparse"):
        from ..serialization import sparse as _sparse  # noqa: F401

        del _sparse
    if modules_available("xarray"):
        from ..serialization import xarray as _xarray  # noqa: F401

        del _xarray
