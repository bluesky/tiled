from pathlib import Path

import h5py

from .datasources import ArraySource
from .in_memory_catalog import Catalog


def access_hdf5_data(name, inner_name, value, size):
    path = Path("example_data")
    filename = f"{name}_{inner_name}.h5"
    file = h5py.File(path / filename, "r")
    return file["data"]


# Build Catalog of Catalogs.
subcatalogs = {}
for name, size, fruit, animal in zip(
    ["tiny", "small", "medium", "large"],
    [3, 100, 1000, 10_000],
    ["apple", "banana", "orange", "grape"],
    ["bird", "cat", "dog", "penguin"],
):
    subcatalogs[name] = Catalog(
        {
            inner_name: ArraySource(access_hdf5_data(name, inner_name, value, size))
            for inner_name, value in zip(["ones", "twos", "threes"], [1, 2, 3])
        },
        metadata={"fruit": fruit, "animal": animal},
    )


hdf5_catalog = Catalog(subcatalogs)
