import h5py
import numpy
import os
from pathlib import Path


def generate_hdf5_data():
    for name, size, fruit, animal in zip(
        ["tiny", "small", "medium", "large"],
        [3, 100, 1000, 10_000],
        ["apple", "banana", "orange", "grape"],
        ["bird", "cat", "dog", "penguin"],
    ):
        for inner_name, value in zip(
            ["ones", "twos", "threes"],
            [1, 2, 3],
        ):
            arr = value * numpy.ones((size, size))
            path = Path("example_data", "hdf5")
            os.makedirs(path, exist_ok=True)
            filename = f"{name}_{inner_name}.h5"
            key = "data"
            with h5py.File(path / filename, "w") as file:
                file.create_dataset(key, data=arr, chunks=True)


def main():
    generate_hdf5_data()


if __name__ == "__main__":
    main()
