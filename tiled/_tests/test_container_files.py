import h5py
import pandas
import pytest
import zarr

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
    z = zarr.open(str(tmpdir / "za.zarr"), "w", shape=(3,), chunks=(3,), dtype="i4")
    z[:] = [1, 2, 3]
    catalog = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        tree(client)
        client["za"].read()
    z.store.close()


@pytest.mark.asyncio
async def test_zarr_group(tmpdir):
    with zarr.open(str(tmpdir / "zg.zarr"), "w") as root:
        root.create_dataset("x", data=[1, 2, 3])
        root.create_dataset("y", data=[4, 5, 6])
    catalog = in_memory(readable_storage=[tmpdir])
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        tree(client)
        client["zg"].export(str(tmpdir / "stuff.h5"))
        client["zg"]["x"].read()
        client["zg"]["y"].read()


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
