import dataclasses
import platform
import random
from pathlib import Path

import numpy
import pytest
import tifffile
import yaml

from ..adapters.tiff import TiffAdapter
from ..catalog import in_memory
from ..catalog.register import (
    Settings,
    create_node_safe,
    group_tiff_sequences,
    identity,
    register,
    register_tiff_sequence,
    skip_all,
    strip_suffixes,
)
from ..catalog.utils import ensure_uri
from ..client import Context, from_context
from ..examples.generate_files import data, df1, generate_files
from ..server.app import build_app
from ..server.schemas import Asset, DataSource, Management


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

    catalog = in_memory(writable_storage=tmpdir)
    with Context.from_app(build_app(catalog)) as context:
        await register(catalog, example_data_dir)

        client = from_context(context)

        # And omits the colliding entries.
        assert "a" not in client

        # Resolve the collision.
        p.unlink()

        # Re-run registration; entry should be there now.
        await register(catalog, example_data_dir)
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
    catalog = in_memory(writable_storage=tmpdir)
    with Context.from_app(build_app(catalog)) as context:
        await register(catalog, tmpdir)
        client = from_context(context)
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

    catalog = in_memory(writable_storage=tmpdir)
    with Context.from_app(build_app(catalog)) as context:
        await register(
            catalog,
            tmpdir,
            mimetype_detection_hook=detect_mimetype,
            key_from_filename=identity,
        )
        client = from_context(context)
        assert set(client) == {"a0", "a.0.asfwoeijviojefeiofw", "c.csv"}


@pytest.mark.asyncio
async def test_skip_all_in_combination(tmpdir):
    "Using skip_all should override defaults, but not hinder other walkers"
    df1.to_csv(Path(tmpdir, "a.csv"))
    Path(tmpdir, "one").mkdir()
    df1.to_csv(Path(tmpdir, "one", "a.csv"))

    for i in range(2):
        tifffile.imwrite(Path(tmpdir, "one", f"image{i:05}.tif"), data)

    catalog = in_memory(writable_storage=tmpdir)
    # By default, both file and tiff sequence are registered.
    with Context.from_app(build_app(catalog)) as context:
        await register(catalog, tmpdir)
        client = from_context(context)
        assert "a" in client
        assert "a" in client["one"]
        assert "image" in client["one"]

    # With skip_all, directories and tiff sequence are registered, but individual files are not
    with Context.from_app(build_app(catalog)) as context:
        await register(catalog, tmpdir, walkers=[group_tiff_sequences, skip_all])
        client = from_context(context)
        assert list(client) == ["one"]
        assert "image" in client["one"]


@pytest.mark.asyncio
async def test_tiff_seq_custom_sorting(tmpdir):
    "Register TIFFs that are not in alphanumeric order."
    N = 10
    ordering = list(range(N))
    random.Random(0).shuffle(ordering)
    files = []
    for i in ordering:
        file = Path(tmpdir, f"image{i:05}.tif")
        files.append(file)
        tifffile.imwrite(file, i * data)

    settings = Settings.init()
    catalog = in_memory(writable_storage=tmpdir)
    with Context.from_app(build_app(catalog)) as context:
        await register_tiff_sequence(
            catalog,
            "image",
            files,
            settings,
        )
        client = from_context(context)
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

    def read_tiff_with_yaml_metadata(
        image_filepath, metadata_filepath, metadata=None, **kwargs
    ):
        with open(metadata_filepath) as file:
            metadata = yaml.safe_load(file)
        return TiffAdapter(image_filepath, metadata=metadata, **kwargs)

    catalog = in_memory(
        writable_storage=tmpdir,
        adapters_by_mimetype={MIMETYPE: read_tiff_with_yaml_metadata},
    )
    with Context.from_app(build_app(catalog)) as context:
        adapter = read_tiff_with_yaml_metadata(image_filepath, metadata_filepath)
        await create_node_safe(
            catalog,
            key="image",
            structure_family=adapter.structure_family,
            metadata=dict(adapter.metadata()),
            specs=adapter.specs,
            data_sources=[
                DataSource(
                    mimetype=MIMETYPE,
                    structure=dataclasses.asdict(adapter.structure()),
                    parameters={},
                    management=Management.external,
                    assets=[
                        Asset(
                            data_uri=str(ensure_uri(str(metadata_filepath))),
                            is_directory=False,
                            parameter="metadata_filepath",
                        ),
                        Asset(
                            data_uri=str(ensure_uri(str(image_filepath))),
                            is_directory=False,
                            parameter="image_filepath",
                        ),
                    ],
                )
            ],
        )
        client = from_context(context)
        assert numpy.array_equal(data, client["image"][:])
        assert client["image"].metadata["test_key"] == 3.0
