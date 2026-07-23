from __future__ import annotations

import os
from pathlib import Path

import numpy

from tiled.client import from_uri

DATASETS = [
    {
        "name": "raw_dataset",
        "kind": "raw",
        "uid": "1f0f5e57-6ab8-4e4e-af42-4d907eb85918",
    },
    {
        "name": "derived_dataset_1",
        "kind": "derived",
        "uid": "717aa522-f8ea-49fa-b667-55bc445621f2",
    },
    {
        "name": "derived_dataset_2",
        "kind": "derived",
        "uid": "f083997c-cfbb-44e0-8989-4beec9c717ea",
    },
    {
        "name": "derived_dataset_3",
        "kind": "derived",
        "uid": "c54e4c14-7f3e-499d-84f6-a4f34fed67d6",
    },
]


def main() -> None:
    base_url = os.getenv("TILED_BASE_URL", "http://127.0.0.1:8000")
    api_key = os.getenv("TILED_API_KEY", "secret")
    storage_root = Path(
        os.getenv("TILED_GRAPHS_STORAGE_ROOT", "example_configs/graphs/data")
    )

    client = from_uri(base_url, api_key=api_key)
    try:
        for dataset in DATASETS:
            name = dataset["name"]
            kind = dataset["kind"]
            uid = dataset["uid"]
            dataset_path = storage_root / name
            if dataset_path.exists():
                print(f"Dataset storage already exists, skipping: {name}")
                continue

            client.write_array(
                key=name,
                array=numpy.ones((4, 4)),
                metadata={
                    "dataset_kind": kind,
                    "tiled_uid": uid,
                },
            )
            print(f"Created catalog dataset: {name} (tiled_uid={uid})")
    finally:
        client.context.close()


if __name__ == "__main__":
    main()
