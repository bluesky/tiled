import os
from pathlib import Path

import h5py
import numpy

from .datasources import ArraySource
from .in_memory_catalog import Catalog


def example_data(name, inner_name, value, size):
    arr = value * numpy.ones((size, size))
    path = Path("example_data")
    os.makedirs(path, exist_ok=True)
    filename = f"{name}_{inner_name}.h5"
    key = "data"
    with h5py.File(path / filename, "w") as file:
        file.create_dataset(key, data=arr)
    return h5py.File(path / filename, "r")[key]


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
            inner_name: ArraySource(example_data(name, inner_name, value, size))
            for inner_name, value in zip(["ones", "twos", "threes"], [1, 2, 3])
        },
        metadata={"fruit": fruit, "animal": animal},
    )
catalog = Catalog(subcatalogs)
