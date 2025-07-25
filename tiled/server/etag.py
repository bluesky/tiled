import importlib.util

if importlib.util.find_spec("dask.tokenize"):
    # moved in dask version 2024.9.0
    from dask.tokenize import normalize_object, normalize_token
    from dask.tokenize import tokenize as dask_tokenize
else:
    from dask.base import normalize_object, normalize_token
    from dask.base import tokenize as dask_tokenize


def tokenize(obj):
    # This just uses dask for now but may evolve to something custom in future.
    return dask_tokenize(obj)


# When dask does not know how to deterministically tokenize something, it falls
# back to normalize_object, which generates a random, nondeterministic token
# (UUID4). Here, we teach it how to tokenize HDF5 datasets. This function will
# only be run if/when an h5py object is encountered.


@normalize_token.register_lazy("h5py")
def register_h5py():
    from pathlib import Path

    import h5py

    @normalize_token.register(h5py.Dataset)
    def normalize_dataset(dataset):
        path = Path(dataset.file.filename)
        if path.is_file() and path.is_absolute():
            return (dataset.file.filename, path.stat().st_mtime, dataset.name)
        # If we reach here, we have some HDF5 file with only a *relative* path or
        # one not backed by a file on disk at all. (It could be BytesIO.)
        return normalize_object(dataset)
