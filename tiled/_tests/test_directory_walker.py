import dataclasses
import platform
import random
from pathlib import Path

import h5py
import numpy
import pytest
import tifffile
import yaml
from starlette.status import HTTP_415_UNSUPPORTED_MEDIA_TYPE

from ..adapters.hdf5 import HDF5Adapter
from ..adapters.tiff import TiffAdapter
from ..adapters.utils import init_adapter_from_catalog
from ..catalog import in_memory
from ..client import Context, from_context
from ..client.register import (
    Settings,
    group_image_sequences,
    identity,
    register,
    register_image_sequence,
    skip_all,
    strip_suffixes,
)
from ..examples.generate_files import data, df1, generate_files
from ..server.app import build_app
from ..structures.array import ArrayStructure
from ..structures.data_source import Asset, DataSource, Management
from ..utils import ensure_uri, path_from_uri
from .utils import fail_with_status_code


@pytest.fixture
def example_data_dir(tmpdir_factory):
    """
    Generate a temporary directory with example files.

    The tmpdir_factory fixture ensures that this directory is cleaned up at test exit.
    """
    tmpdir = tmpdir_factory.mktemp("example_files")
    generate_files(tmpdir)
    return tmpdir


@pytest.mark.xfail(
    platform.system() == "Windows",
    reason="file cannot be removed while being used",
    raises=PermissionError,
)
@pytest.mark.asyncio
async def test_collision(example_data_dir, tmpdir):
    """Test that files which produce key collisions are ignored until the collision is resolved."""
    # Add a.tiff which will collide with a.tif.
    p = Path(example_data_dir, "a.tiff")
    tifffile.imwrite(str(p), data)

    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, example_data_dir)

        # And omits the colliding entries.
        assert "a" not in client

        # Resolve the collision.
        p.unlink()

        # Re-run registration; entry should be there now.
        await register(client, example_data_dir)
        assert "a" in client


@pytest.mark.parametrize(
    ("filename", "expected"), [("a.txt", "a"), ("a.tar.gz", "a"), ("a", "a")]
)
def test_strip_suffixes(filename, expected):
    actual = strip_suffixes(filename)
    assert actual == expected


@pytest.mark.asyncio
async def test_same_filename_separate_directory(tmpdir):
    "Two files with the same name in separate directories should not collide."
    Path(tmpdir, "one").mkdir()
    Path(tmpdir, "two").mkdir()
    df1.to_csv(Path(tmpdir, "one", "a.csv"))
    df1.to_csv(Path(tmpdir, "two", "a.csv"))
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        assert "a" in client["one"]
        assert "a" in client["two"]


@pytest.mark.asyncio
async def test_mimetype_detection_hook(tmpdir):
    content = "a, b, c\n1, 2 ,3\n4, 5, 6\n"
    with open(Path(tmpdir / "a0"), "w") as file:
        file.write(content)
    with open(Path(tmpdir / "b0"), "w") as file:
        file.write(content)
    with open(Path(tmpdir / "c.csv"), "w") as file:
        file.write(content)
    with open(Path(tmpdir / "a.0.asfwoeijviojefeiofw"), "w") as file:
        file.write(content)
    with open(Path(tmpdir / "b.0.asfwoeijviojefeiofw"), "w") as file:
        file.write(content)

    def detect_mimetype(path, mimetype):
        filename = Path(path).name
        # If detection based on file extension worked,
        # we should get that in the mimetype. Otherwise,
        # mimetype should be None.
        if filename.endswith(".csv"):
            assert mimetype == "text/csv"
        else:
            assert mimetype is None
        if filename.startswith("a"):
            return "text/csv"
        return mimetype

    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(
            client,
            tmpdir,
            mimetype_detection_hook=detect_mimetype,
            key_from_filename=identity,
        )
        assert set(client) == {"a0", "a.0.asfwoeijviojefeiofw", "c.csv"}


@pytest.mark.asyncio
async def test_skip_all_in_combination(tmpdir):
    "Using skip_all should override defaults, but not hinder other walkers"
    df1.to_csv(Path(tmpdir, "a.csv"))
    Path(tmpdir, "one").mkdir()
    df1.to_csv(Path(tmpdir, "one", "a.csv"))

    for i in range(2):
        tifffile.imwrite(Path(tmpdir, "one", f"image{i:05}.tif"), data)

    catalog = in_memory(writable_storage=str(tmpdir))
    # By default, both file and tiff sequence are registered.
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir)
        assert "a" in client
        assert "a" in client["one"]
        assert "image" in client["one"]

    # With skip_all, directories and tiff sequence are registered, but individual files are not
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register(client, tmpdir, walkers=[group_image_sequences, skip_all])
        assert list(client) == ["one"]
        assert "image" in client["one"]


@pytest.mark.asyncio
async def test_tiff_seq_custom_sorting(tmpdir):
    "Register images that are not in alphanumeric order."
    N = 10
    ordering = list(range(N))
    random.Random(0).shuffle(ordering)
    files = []
    for i in ordering:
        file = Path(tmpdir, f"image{i:05}.tif")
        files.append(file)
        # data is a block of ones
        tifffile.imwrite(file, i * data)

    settings = Settings.init()
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        await register_image_sequence(
            client,
            "image",
            files,
            settings,
        )
        # We are being a bit clever here.
        # Each image in this image series has pixels with a constant value, and
        # that value matches the image's position in the sequence enumerated by
        # `ordering`. We pick out one pixel and check that its value matches
        # the corresponding value in `ordering`.
        actual = list(client["image"][:, 0, 0])
        assert actual == ordering


@pytest.mark.asyncio
async def test_image_file_with_sidecar_metadata_file(tmpdir):
    "Create one Node from two different types of files."
    MIMETYPE = "multipart/related;type=application/x-tiff-with-yaml"
    image_filepath = Path(tmpdir, "image.tif")
    tifffile.imwrite(image_filepath, data)
    metadata_filepath = Path(tmpdir, "metadata.yml")
    metadata = {"test_key": 3.0}
    with open(metadata_filepath, "w") as file:
        yaml.dump(metadata, file)

    class TiffAdapterWithSidecar(TiffAdapter):
        def __init__(self, image_uri, metadata_uri, metadata=None, **kwargs):
            with open(path_from_uri(metadata_uri)) as file:
                metadata = yaml.safe_load(file)

            super().__init__(image_uri, metadata=metadata, **kwargs)

        @classmethod
        def from_catalog(
            cls,
            data_source,
            node,
            /,
            **kwargs,
        ):
            return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    catalog = in_memory(
        writable_storage=str(tmpdir),
        adapters_by_mimetype={MIMETYPE: TiffAdapterWithSidecar},
    )
    with Context.from_app(build_app(catalog)) as context:
        adapter = TiffAdapterWithSidecar(
            ensure_uri(image_filepath), ensure_uri(metadata_filepath)
        )
        client = from_context(context)
        client.new(
            key="image",
            structure_family=adapter.structure_family,
            metadata=dict(adapter.metadata()),
            specs=adapter.specs,
            data_sources=[
                DataSource(
                    structure_family=adapter.structure_family,
                    mimetype=MIMETYPE,
                    structure=dataclasses.asdict(adapter.structure()),
                    parameters={},
                    management=Management.external,
                    assets=[
                        Asset(
                            data_uri=ensure_uri(metadata_filepath),
                            is_directory=False,
                            parameter="metadata_uri",
                        ),
                        Asset(
                            data_uri=ensure_uri(image_filepath),
                            is_directory=False,
                            parameter="image_uri",
                        ),
                    ],
                )
            ],
        )
        assert numpy.array_equal(data, client["image"][:])
        assert client["image"].metadata["test_key"] == 3.0


@pytest.mark.asyncio
async def test_hdf5_virtual_datasets(tmpdir):
    # A virtual database comprises one master file and N data files. The master
    # file must be handed to the Adapter for opening. The data files are not
    # handled directly by the Adapter but they still ought to be tracked as
    # Assets for purposes of data movement, accounting for data size, etc.
    # This is why they are Assets with parameter=NULL/None, Assets not used
    # directly by the Adapter.

    # One could do one-dataset-per-directory. But like TIFF series in practice
    # they are often mixed, so we address that general case and track them at
    # the per-file level.

    # Contrast this to Zarr, where the files involves are always bundled by
    # directory. We track Zarr at the directory level.

    layout = h5py.VirtualLayout(shape=(4, 100), dtype="i4")

    data_filepaths = []
    for n in range(1, 5):
        filepath = Path(tmpdir, f"{n}.h5")
        data_filepaths.append(filepath)
        vsource = h5py.VirtualSource(filepath, "data", shape=(100,))
        layout[n - 1] = vsource

    # Add virtual dataset to output file
    filepath = Path(tmpdir, "VDS.h5")
    with h5py.File(filepath, "w", libver="latest") as file:
        file.create_virtual_dataset("data", layout, fillvalue=-5)

    assets = [
        Asset(
            data_uri=str(ensure_uri(str(fp))),
            is_directory=False,
            parameter=None,  # an indirect dependency
        )
        for fp in data_filepaths
    ]
    assets.append(
        Asset(
            data_uri=ensure_uri(filepath),
            is_directory=False,
            parameter="data_uris",
        )
    )
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        adapter = HDF5Adapter.from_uris(ensure_uri(filepath))
        client = from_context(context)
        client.new(
            key="VDS",
            structure_family=adapter.structure_family,
            metadata=dict(adapter.metadata()),
            specs=adapter.specs,
            data_sources=[
                DataSource(
                    structure_family=adapter.structure_family,
                    mimetype="application/x-hdf5",
                    structure=None,
                    parameters={},
                    management=Management.external,
                    assets=assets,
                )
            ],
        )
        client["VDS"]["data"][:]


def test_unknown_mimetype(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        asset = Asset(
            data_uri=ensure_uri(tmpdir / "test.does_not_exist"),
            is_directory=False,
            parameter="test",
        )
        with fail_with_status_code(HTTP_415_UNSUPPORTED_MEDIA_TYPE):
            client.new(
                key="x",
                structure_family="array",
                metadata={},
                specs=[],
                data_sources=[
                    DataSource(
                        structure_family="array",
                        mimetype="application/x-does-not-exist",
                        structure=None,
                        parameters={},
                        management=Management.external,
                        assets=[asset],
                    )
                ],
            )


def test_one_asset_two_data_sources(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    with Context.from_app(build_app(catalog)) as context:
        client = from_context(context)
        asset = Asset(
            data_uri=ensure_uri(tmpdir / "test.tiff"),
            is_directory=False,
            parameter="data_uri",
            num=None,
        )
        for key in ["x", "y"]:
            client.new(
                key=key,
                structure_family="array",
                metadata={},
                specs=[],
                data_sources=[
                    DataSource(
                        structure_family="array",
                        mimetype="image/tiff",
                        structure=ArrayStructure.from_array(numpy.empty((5, 7))),
                        parameters={},
                        management=Management.external,
                        assets=[asset],
                    )
                ],
            )
