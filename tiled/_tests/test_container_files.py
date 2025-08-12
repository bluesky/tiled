from pathlib import Path

import h5py
import numpy as np
import pandas
import pytest
import zarr

from ..adapters.zarr import ZARR_LIB_V2
from ..catalog import in_memory
from ..client import Context, from_context, tree
from ..client.register import register
from ..server.app import build_app


@pytest.mark.asyncio
async def test_excel(tmpdir):
    df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_excel(tmpdir / "spreadsheet.xlsx")
    catalog = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        tree(client)
        client["spreadsheet"]["Sheet1"].read()


@pytest.mark.asyncio
async def test_zarr_array(tmpdir):
    z = zarr.open(
        str(tmpdir / "za.zarr"), mode="w", shape=(3,), chunks=(3,), dtype="i4"
    )
    z[:] = [1, 2, 3]
    catalog = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        tree(client)
        client["za"].read()
    z.store.close()


@pytest.mark.asyncio
async def test_zarr_group(tmp_path: Path):
    root = zarr.open(tmp_path / "zg.zarr", mode="w")
    x_array = np.array([1, 2, 3])
    y_array = np.array([4, 5, 6])

    if ZARR_LIB_V2:
        root.create_dataset("x", data=x_array)
        root.create_dataset("y", data=y_array)
    else:
        root.create_array(name="x", data=x_array)
        root.create_array(name="y", data=y_array)

    catalog = in_memory(readable_storage=[tmp_path])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmp_path)
        tree(client)
        # Normally, zarr would have 'attributes' stored as internal dictionary in the
        # metadata, but HDF5 does not support nested dictionaries.
        client["zg"].replace_metadata(
            {"attributes": "", "zarr_format": 2 if ZARR_LIB_V2 else 3}
        )
        client["zg"].export(tmp_path / "stuff.h5")
        assert client["zg"]["x"].read().data == x_array
        assert client["zg"]["y"].read().data == y_array


@pytest.mark.asyncio
async def test_hdf5(tmpdir, buffer):
    with h5py.File(str(tmpdir / "h.h5"), "w") as file:
        file["x"] = [1, 2, 3]
        group = file.create_group("g")
        group["y"] = [4, 5, 6]
    catalog = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        tree(client)
        client["h"]["x"].read()
        client["h"]["g"]["y"].read()

        client.export(buffer, format="application/json")
