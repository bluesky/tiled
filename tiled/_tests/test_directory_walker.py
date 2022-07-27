import shutil
import time
from pathlib import Path

import numpy
import pytest
import tifffile

from ..adapters.array import ArrayAdapter
from ..adapters.files import Change, identity, strip_suffixes
from ..client import from_config
from ..examples.generate_files import data, df1, generate_files
from .utils import force_update


@pytest.fixture
def example_data_dir(tmpdir_factory):
    """
    Generate a temporary directory with example files.

    The tmpdir_factory fixture ensures that this directory is cleaned up at test exit.
    """
    tmpdir = tmpdir_factory.mktemp("temp")
    generate_files(tmpdir)
    return tmpdir


def test_from_directory(example_data_dir):
    """Tests that from_config with a Tree from a directory produces a node"""
    config = {
        "trees": [
            {
                "tree": "tiled.adapters.files:DirectoryAdapter.from_directory",
                "path": "/",
                "args": {"directory": str(example_data_dir)},
            }
        ]
    }
    client = from_config(config)
    arr = client["c"].read()
    assert isinstance(arr, numpy.ndarray)


def test_files_config_alias(example_data_dir):
    """Test the config alias 'files' for 'tiled.adapters.files:DirectoryAdapter.from_directory"""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }
    # Testing successful construction is sufficient.
    from_config(config)


def test_item_added(example_data_dir):
    """Test that an added file or directory is detected."""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }
    client = from_config(config)

    # Generate new files and directories.
    df1.to_csv(Path(example_data_dir, "added_file_top_level.csv"))
    df1.to_csv(Path(example_data_dir, "more", "added_file_in_subdir.csv"))
    df1.to_csv(
        Path(example_data_dir, "more", "even_more", "added_file_in_subsubdir.csv")
    )
    p = Path(example_data_dir, "more", "new_subdir", "added_file_in_new_subdir.csv")
    p.parent.mkdir()
    df1.to_csv(p)

    force_update(client)

    assert "added_file_top_level" in client
    assert "added_file_in_subdir" in client["more"]
    assert "added_file_in_subsubdir" in client["more"]["even_more"]
    assert "new_subdir" in client["more"]
    assert "added_file_in_new_subdir" in client["more"]["new_subdir"]


def test_item_removed(example_data_dir):
    """Test that file and directory removal are detected."""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }
    client = from_config(config)

    # Check that the items are here to begin with.
    assert "c" in client
    assert "more" in client

    # Remove things.
    shutil.rmtree(Path(example_data_dir, "more", "even_more"))
    Path(example_data_dir, "c.tif").unlink()
    Path(example_data_dir, "more", "d.tif").unlink()

    force_update(client)
    assert "c" not in client
    assert "even_more" not in client
    assert "d" not in client["more"]


def test_collision_at_startup(example_data_dir):
    """Test that files which produce key collisions are ignored until the collision is resolved."""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }

    # Add a.tiff which will collide with a.tif.
    p = Path(example_data_dir, "a.tiff")
    tifffile.imwrite(str(p), data)

    with pytest.warns(UserWarning):
        # Tree warns about collision.
        client = from_config(config)

    # And omits the colliding entries.
    assert "a" not in client

    # Resolve the collision.
    p.unlink()

    force_update(client)
    assert "a" in client


def test_collision_after_startup(example_data_dir):
    """Test that files which produce key collisions are ignored until the collision is resolved."""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }

    client = from_config(config)

    assert "a" in client

    # Add a.tiff which will collide with a.tif.
    p = Path(example_data_dir, "a.tiff")
    with pytest.warns(UserWarning):
        tifffile.imwrite(str(p), data)
        force_update(client)

    assert "a" not in client

    # Resolve the collision.
    p.unlink()

    force_update(client)
    assert "a" in client


def test_remove_and_re_add(example_data_dir):
    """Test that removing and re-adding a file does not constitute a collision."""
    config = {
        "trees": [
            {"tree": "files", "path": "/", "args": {"directory": str(example_data_dir)}}
        ]
    }

    client = from_config(config)

    assert "a" in client

    # Remove a file.
    p = Path(example_data_dir, "a.tif")
    p.unlink()

    # Confirm it is gone.
    force_update(client)
    assert "a" not in client

    # Add it back.
    tifffile.imwrite(str(p), data)

    # Confirm it is back (no spurious collision).
    force_update(client)
    assert "a" in client


@pytest.mark.parametrize(
    ("filename", "expected"), [("a.txt", "a"), ("a.tar.gz", "a"), ("a", "a")]
)
def test_strip_suffixes(filename, expected):
    actual = strip_suffixes(filename)
    assert actual == expected


def test_same_filename_separate_directory(tmpdir):
    "Two files with the same name in separate directories should not collide."
    Path(tmpdir, "one").mkdir()
    Path(tmpdir, "two").mkdir()
    df1.to_csv(Path(tmpdir, "one", "a.csv"))
    df1.to_csv(Path(tmpdir, "two", "a.csv"))
    config = {"trees": [{"tree": "files", "path": "/", "args": {"directory": tmpdir}}]}
    client = from_config(config)
    assert "a" in client["one"]
    assert "a" in client["two"]


def test_subdirectory_handler(tmpdir):

    changes = []  # accumulate (kind, path) changes

    def get_changes_callback():
        return changes.append

    def example_subdirectory_handler(path):

        if "separately_managed" == path.name:
            # In this dummy example, ignore the files in this directory
            # and just return a constant array.
            dummy = ArrayAdapter.from_array(data)
            dummy.get_changes_callback = get_changes_callback
            return dummy

    Path(tmpdir, "separately_managed").mkdir()
    Path(tmpdir, "individual_files").mkdir()
    df1.to_csv(Path(tmpdir, "individual_files", "a.csv"))
    df1.to_csv(Path(tmpdir, "individual_files", "b.csv"))
    df1.to_csv(Path(tmpdir, "separately_managed", "a.csv"))
    df1.to_csv(Path(tmpdir, "separately_managed", "b.csv"))
    config = {
        "trees": [
            {
                "tree": "files",
                "path": "/",
                "args": {
                    "directory": tmpdir,
                    "subdirectory_handler": example_subdirectory_handler,
                },
            }
        ]
    }
    client = from_config(config)
    client["individual_files"]
    client["individual_files"]["a"]
    client["individual_files"]["b"]
    arr = client["separately_managed"].read()
    assert isinstance(arr, numpy.ndarray)

    df1.to_csv(Path(tmpdir, "individual_files", "c.csv"))
    force_update(client)
    assert "c" in client["individual_files"]

    # Adding, changing, or, removing files should notify the handler.
    df1.to_csv(Path(tmpdir, "separately_managed", "c.csv"))  # added
    df1.to_csv(Path(tmpdir, "separately_managed", "a.csv"))  # modified
    time.sleep(0.5)  # Give slow CI filesystem time to catch up.
    force_update(client)

    Path(tmpdir, "separately_managed", "c.csv").unlink()  # removed
    # Add a new file in a new subdirectory.
    Path(tmpdir, "separately_managed", "new_subdir").mkdir()
    df1.to_csv(Path(tmpdir, "separately_managed", "new_subdir", "d.csv"))
    time.sleep(0.5)  # Give slow CI filesystem time to catch up.
    force_update(client)

    expected_first_batch = [
        (Change.added, Path("c.csv")),
        (Change.modified, Path("a.csv")),
    ]
    expected_second_batch = [
        (Change.deleted, Path("c.csv")),
        (Change.added, Path("new_subdir", "d.csv")),
    ]
    # First batch of changes reported
    assert set(changes[0]) == set(expected_first_batch)
    # Second batch of changes reported
    assert set(changes[1]) == set(expected_second_batch)


def test_sort(example_data_dir):
    """
    This should do nothing because the nodes have no metatdata.

    The test is just that nothing errors out.
    """
    config = {
        "trees": [
            {
                "tree": "tiled.adapters.files:DirectoryAdapter.from_directory",
                "path": "/",
                "args": {"directory": str(example_data_dir)},
            }
        ]
    }
    client = from_config(config)
    list(client.sort(("does_not_exsit", 1)))


def test_mimetype_detection_hook(tmpdir):
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

    config = {
        "trees": [
            {
                "tree": "tiled.adapters.files:DirectoryAdapter.from_directory",
                "path": "/",
                "args": {
                    "directory": str(tmpdir),
                    "mimetype_detection_hook": detect_mimetype,
                    "key_from_filename": identity,
                },
            }
        ]
    }
    client = from_config(config)
    assert set(client) == {"a0", "a.0.asfwoeijviojefeiofw", "c.csv"}
