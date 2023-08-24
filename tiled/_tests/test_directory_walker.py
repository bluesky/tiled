import platform
from pathlib import Path

import pytest
import tifffile

from ..catalog import in_memory
from ..catalog.register import (
    identity,
    register,
    skip_all,
    strip_suffixes,
    tiff_sequence,
)
from ..client import Context, from_context
from ..examples.generate_files import data, df1, generate_files
from ..server.app import build_app


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

    tree = in_memory()
    with Context.from_app(build_app(tree)) as context:
        await register(tree, example_data_dir)

        client = from_context(context)

        # And omits the colliding entries.
        assert "a" not in client

        # Resolve the collision.
        p.unlink()

        # Re-run registration; entry should be there now.
        await register(tree, example_data_dir)
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
    tree = in_memory()
    with Context.from_app(build_app(tree)) as context:
        await register(tree, tmpdir)
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

    tree = in_memory()
    with Context.from_app(build_app(tree)) as context:
        await register(
            tree,
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

    tree = in_memory()
    # By default, both file and tiff sequence are registered.
    with Context.from_app(build_app(tree)) as context:
        await register(tree, tmpdir)
        client = from_context(context)
        assert "a" in client
        assert "a" in client["one"]
        assert "image" in client["one"]

    # With skip_all, directories and tiff sequence are registered, but individual files are not
    with Context.from_app(build_app(tree)) as context:
        await register(tree, tmpdir, walkers=[tiff_sequence, skip_all])
        client = from_context(context)
        assert list(client) == ["one"]
        assert "image" in client["one"]
