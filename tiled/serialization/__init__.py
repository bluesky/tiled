from ..utils import modules_available


def register_builtin_serializers():
    """
    Register built-in serializers for each structure if its associated module(s) are installed.
    """
    # Each submodule in ..serialization registers serializers on import.
    # Some are conditional on the availability of particular I/O libraries.
    from ..serialization import container as _container

    del _container
    if modules_available("numpy", "dask.array"):
        from ..serialization import array as _array

        del _array
    if modules_available("awkward"):
        from ..serialization import awkward as _awkward

        del _awkward
    if modules_available("pandas", "pyarrow", "dask.dataframe"):
        from ..serialization import table as _table

        del _table

    if modules_available("pandas"):
        from ..serialization import xdi as _xdi

        del _xdi
    if modules_available("ragged"):
        from ..serialization import ragged as _ragged

        del _ragged
    if modules_available("sparse"):
        from ..serialization import sparse as _sparse

        del _sparse
    if modules_available("xarray"):
        from ..serialization import xarray as _xarray

        del _xarray
