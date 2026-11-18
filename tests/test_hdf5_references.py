"""
An HDF5 file can refer to other files: through external links, through virtual
datasets, and through datasets whose raw data is stored in another file.

A server follows such references only into its readable storage.
"""

import os
import sys
from unittest import mock

import h5py
import numpy
import pytest
from starlette.status import HTTP_410_GONE

import tiled.adapters.hdf5
from tiled.adapters.hdf5 import HDF5Adapter
from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management
from tiled.utils import BrokenLink, ensure_uri

INSIDE = 1  # the value of data in readable storage
OUTSIDE = 7  # the value of data outside of readable storage
SECRET = b"secret!!"


def write_dataset(path, value, name="data"):
    with h5py.File(path, "a") as file:
        file[name] = numpy.full(3, value)


def add_virtual(file, name, source_file, source_dataset="data", shape=(3,)):
    layout = h5py.VirtualLayout(shape=shape, dtype="i8")
    layout[...] = h5py.VirtualSource(source_file, source_dataset, shape=shape)
    file.create_virtual_dataset(name, layout, fillvalue=-1)


def add_virtual_printf(file, name, source_file, source_dataset="data"):
    """Add a virtual dataset of unlimited length, one row per source file.

    The source name contains %b, which stands for 0, 1, 2, ... up to the first
    file that does not exist.
    """
    space = h5py.h5s.create_simple((0, 3), (h5py.h5s.UNLIMITED, 3))
    virtual_space = h5py.h5s.create_simple((0, 3), (h5py.h5s.UNLIMITED, 3))
    virtual_space.select_hyperslab(
        start=(0, 0), count=(h5py.h5s.UNLIMITED, 1), stride=(1, 1), block=(1, 3)
    )
    source_space = h5py.h5s.create_simple((1, 3))
    source_space.select_all()
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    dcpl.set_virtual(
        virtual_space, source_file.encode(), source_dataset.encode(), source_space
    )
    h5py.h5d.create(file.id, name.encode(), h5py.h5t.NATIVE_INT64, space, dcpl=dcpl)


def add_external_storage(file, name, path):
    file.create_dataset(
        name, shape=(len(SECRET),), dtype="u1", external=[(str(path), 0, len(SECRET))]
    )


@pytest.fixture
def layout(tmp_path, monkeypatch):
    """
    readable/    the readable storage, holding the file under test
    outside/     not readable
    cwd/         the working directory of the server, not readable
    """
    readable = tmp_path / "readable"
    outside = tmp_path / "outside"
    cwd = tmp_path / "cwd"
    for directory in (readable, outside, cwd):
        directory.mkdir()
    write_dataset(readable / "inside.h5", INSIDE)
    write_dataset(outside / "secret.h5", OUTSIDE)
    write_dataset(cwd / "only_in_cwd.h5", OUTSIDE)
    (outside / "secret.txt").write_bytes(SECRET)
    (readable / "raw.bin").write_bytes(SECRET)
    # A file in readable storage that passes on what is outside of it.
    with h5py.File(readable / "relay.h5", "w") as file:
        add_external_storage(file, "external_storage", outside / "secret.txt")
        add_virtual(file, "virtual", str(outside / "secret.h5"))
        file["link"] = h5py.ExternalLink(str(outside / "secret.h5"), "/data")
    # Blocks of a virtual dataset with a printf-style source name.
    for path, value in [
        (readable / "block-0.h5", INSIDE),
        (readable / "block-1.h5", INSIDE),
        (outside / "block.h5", OUTSIDE),
    ]:
        with h5py.File(path, "w") as file:
            file["data"] = numpy.full((1, 3), value)
    monkeypatch.chdir(cwd)
    return tmp_path


def build(layout, name):
    "Write readable/master.h5 with one entry, `x`. Return the URI to open it by."
    readable, outside = layout / "readable", layout / "outside"
    path = readable / "master.h5"
    uri = ensure_uri(path)
    with h5py.File(path, "w", libver="latest") as file:
        if name == "plain":
            file["x"] = numpy.full(3, INSIDE)
        elif name == "soft-link":
            file["target"] = numpy.full(3, INSIDE)
            file["x"] = h5py.SoftLink("/target")
        elif name == "link-inside":
            file["x"] = h5py.ExternalLink("inside.h5", "/data")
        elif name == "link-inside-absolute":
            file["x"] = h5py.ExternalLink(str(readable / "inside.h5"), "/data")
        elif name == "virtual-inside":
            add_virtual(file, "x", "inside.h5")
        elif name == "virtual-inside-absolute":
            add_virtual(file, "x", str(readable / "inside.h5"))
        elif name == "virtual-missing-absolute-found-next-to-master":
            # HDF5 falls back to the last part of a name that does not exist.
            add_virtual(file, "x", "/no/such/directory/inside.h5")
        elif name == "virtual-same-file":
            file["target"] = numpy.full(3, INSIDE)
            add_virtual(file, "x", ".", "target")
        elif name == "virtual-printf-inside":
            add_virtual_printf(file, "x", "block-%b.h5")
        elif name == "virtual-printf-block-outside":
            os.symlink(outside / "block.h5", readable / "block-2.h5")
            add_virtual_printf(file, "x", "block-%b.h5")
        elif name == "virtual-missing-source":
            add_virtual(file, "x", "no-such-file.h5")
        elif name == "external-storage-outside":
            add_external_storage(file, "x", outside / "secret.txt")
        elif name == "external-storage-inside":
            add_external_storage(file, "x", readable / "raw.bin")
        elif name == "external-storage-relative":
            add_external_storage(file, "x", "raw.bin")
        elif name == "link-outside":
            file["x"] = h5py.ExternalLink(str(outside / "secret.h5"), "/data")
        elif name == "link-outside-relative":
            file["x"] = h5py.ExternalLink("../outside/secret.h5", "/data")
        elif name == "link-found-in-cwd":
            file["x"] = h5py.ExternalLink("only_in_cwd.h5", "/data")
        elif name == "link-to-group-outside":
            file["g"] = h5py.ExternalLink(str(outside / "secret.h5"), "/")
            file["x"] = h5py.SoftLink("/g/data")
        elif name == "link-to-relay-external-storage":
            file["x"] = h5py.ExternalLink("relay.h5", "/external_storage")
        elif name == "link-to-relay-link":
            file["x"] = h5py.ExternalLink("relay.h5", "/link")
        elif name == "link-to-relay-virtual":
            file["x"] = h5py.ExternalLink("relay.h5", "/virtual")
        elif name == "virtual-outside":
            add_virtual(file, "x", str(outside / "secret.h5"))
        elif name == "virtual-outside-relative":
            add_virtual(file, "x", "../outside/secret.h5")
        elif name == "virtual-found-in-cwd":
            add_virtual(file, "x", "only_in_cwd.h5")
        elif name == "virtual-missing-absolute-found-in-cwd":
            add_virtual(file, "x", "/no/such/directory/only_in_cwd.h5")
        elif name == "link-missing-absolute-found-in-cwd":
            file["x"] = h5py.ExternalLink("/no/such/directory/only_in_cwd.h5", "/data")
        elif name == "virtual-inside-shadowed-in-cwd":
            # The same name exists where the server runs. HDF5 would not read
            # that one, but the server does not rely on the search order.
            write_dataset(layout / "cwd" / "inside.h5", OUTSIDE)
            add_virtual(file, "x", "inside.h5")
        elif name == "virtual-of-relay-external-storage":
            add_virtual(file, "x", "relay.h5", "external_storage", shape=(8,))
        elif name == "virtual-of-relay-link":
            add_virtual(file, "x", "relay.h5", "link")
        elif name == "virtual-of-relay-virtual":
            add_virtual(file, "x", "relay.h5", "virtual")
        elif name == "virtual-of-link-through-readable-symlink":
            os.symlink(outside / "secret.h5", readable / "disguised.h5")
            add_virtual(file, "x", "disguised.h5")
        elif name == "virtual-next-to-link-target":
            # HDF5 also looks next to the file that master.h5 links to.
            (readable / "sub").mkdir()
            os.symlink(outside / "secret.h5", readable / "sub" / "leaf.h5")
            add_virtual(file, "x", "leaf.h5")
        elif name == "virtual-next-to-master-through-link-and-dot-dot":
            # HDF5 looks next to the master file by the name it was opened
            # with. Normalized, the name below is readable/master.h5. HDF5
            # follows readable/a to readable/b/c, so it opens
            # readable/b/master.h5 and looks for leaf.h5 in readable/b.
            if sys.platform == "win32":
                pytest.skip("Windows removes '..' from a path before following links.")
            (readable / "b" / "c").mkdir(parents=True)
            os.symlink(readable / "b" / "c", readable / "a")
            os.symlink(outside / "secret.h5", readable / "b" / "leaf.h5")
            add_virtual(file, "x", "leaf.h5")
            uri = ensure_uri(readable / "a" / ".." / "master.h5")
        elif name == "virtual-nested-inside":
            with h5py.File(readable / "mid.h5", "w") as mid:
                add_virtual(mid, "data", "inside.h5")
            add_virtual(file, "x", "mid.h5")
        else:
            raise ValueError(name)
    # The file is closed; it can be moved.
    if name == "virtual-next-to-link-target":
        os.rename(path, readable / "sub" / "real.h5")
        os.symlink(readable / "sub" / "real.h5", path)
    elif name == "virtual-next-to-master-through-link-and-dot-dot":
        os.rename(path, readable / "b" / "master.h5")
    return uri


ALLOWED = [
    "plain",
    "soft-link",
    "link-inside",
    "link-inside-absolute",
    "virtual-inside",
    "virtual-inside-absolute",
    "virtual-same-file",
    "virtual-printf-inside",
    "virtual-missing-absolute-found-next-to-master",
    "virtual-nested-inside",
]

REFUSED = [
    "external-storage-outside",
    "external-storage-inside",
    "external-storage-relative",
    "link-outside",
    "link-outside-relative",
    "link-found-in-cwd",
    "link-to-group-outside",
    "link-to-relay-external-storage",
    "link-to-relay-link",
    "link-to-relay-virtual",
    "virtual-outside",
    "virtual-outside-relative",
    "virtual-found-in-cwd",
    "virtual-missing-absolute-found-in-cwd",
    "link-missing-absolute-found-in-cwd",
    "virtual-inside-shadowed-in-cwd",
    "virtual-of-relay-external-storage",
    "virtual-of-relay-link",
    "virtual-of-relay-virtual",
    "virtual-of-link-through-readable-symlink",
    "virtual-printf-block-outside",
    "virtual-next-to-link-target",
    "virtual-next-to-master-through-link-and-dot-dot",
]


def read(uri, readable_directories):
    adapter = HDF5Adapter.from_uris(
        uri, dataset="x", readable_directories=readable_directories
    )
    return numpy.asarray(adapter.read())


@pytest.mark.parametrize("name", ALLOWED)
def test_reference_into_readable_storage_is_followed(layout, name):
    data = read(build(layout, name), [layout / "readable"])

    assert (data == INSIDE).all()


def test_missing_virtual_source_reads_as_fill_value(layout):
    data = read(build(layout, "virtual-missing-source"), [layout / "readable"])

    assert (data == -1).all()


@pytest.mark.parametrize("name", REFUSED)
def test_reference_out_of_readable_storage_is_refused(layout, name):
    uri = build(layout, name)

    with pytest.raises(BrokenLink):
        read(uri, [layout / "readable"])


@pytest.mark.parametrize("name", REFUSED)
def test_refused_reference_is_a_broken_link_in_the_tree(layout, name):
    "The entry is listed, and cannot be opened, like a link whose target is gone."
    uri = build(layout, name)

    tree = HDF5Adapter.from_uris(uri, readable_directories=[layout / "readable"])

    assert list(tree) == ["x"] or set(tree) == {"g", "x"}
    with pytest.raises(BrokenLink):
        numpy.asarray(tree["x"].read())


@pytest.mark.parametrize("name", ALLOWED + REFUSED)
def test_adapter_used_directly_is_not_restricted(layout, name):
    "Without readable directories, as when a user opens their own file."
    if name == "external-storage-relative":
        pytest.skip("HDF5 looks for this file in the working directory.")
    data = read(build(layout, name), None)

    assert data.shape[-1] in (3, len(SECRET))


@pytest.mark.parametrize("empty_prefix_first", [False, True])
def test_virtual_source_prefix_from_environment_is_considered(
    layout, monkeypatch, empty_prefix_first
):
    "HDF5_VDS_PREFIX makes HDF5 look elsewhere first. It may list several places."
    uri = build(layout, "virtual-inside")
    write_dataset(layout / "outside" / "inside.h5", OUTSIDE)
    prefixes = [str(layout / "outside")]
    if empty_prefix_first:
        (layout / "empty").mkdir()
        prefixes.insert(0, str(layout / "empty"))
    monkeypatch.setenv("HDF5_VDS_PREFIX", os.pathsep.join(prefixes))

    with pytest.raises(BrokenLink):
        read(uri, [layout / "readable"])


def test_group_reached_through_refused_link_is_not_listed(layout):
    uri = build(layout, "link-to-group-outside")

    tree = HDF5Adapter.from_uris(uri, readable_directories=[layout / "readable"])

    assert set(tree) == {"g", "x"}
    with pytest.raises(BrokenLink):
        tree["g"]


@pytest.mark.parametrize(
    "name, change",
    [
        ("virtual-inside", "replace"),
        ("virtual-nested-inside", "replace"),
        ("virtual-inside", "rewrite-in-place"),
        ("virtual-missing-source", "appear"),
    ],
    ids=[
        "source-replaced",
        "source-of-source-replaced",
        "source-rewritten-in-place",
        "missing-source-appears",
    ],
)
def test_virtual_sources_are_checked_once_until_a_file_changes(layout, name, change):
    """A virtual dataset is checked again when any file it depends on changes.

    That includes the sources of its sources, a file rewritten in place with
    its inode, size and modification time kept, and a missing source that
    appears.
    """
    if change == "rewrite-in-place" and sys.platform == "win32":
        pytest.skip("On Windows, st_ctime is the creation time, which does not change.")
    uri = build(layout, name)
    readable, outside = layout / "readable", layout / "outside"
    source = readable / ("no-such-file.h5" if change == "appear" else "inside.h5")
    if change == "rewrite-in-place":
        # Make the benign source larger than its replacement will be.
        with h5py.File(source, "a") as file:
            file.attrs["padding"] = numpy.zeros(4096, dtype="u1")
    expected = -1 if change == "appear" else INSIDE

    with mock.patch(
        "tiled.adapters.hdf5._open_source", wraps=tiled.adapters.hdf5._open_source
    ) as open_source:
        assert (read(uri, [readable]) == expected).all()
        opened = open_source.call_count
        assert (read(uri, [readable]) == expected).all()
        assert open_source.call_count == opened  # The check was not repeated.

        # The source now passes on outside data.
        if change == "rewrite-in-place":
            stat = os.stat(source)
            replacement = outside / "replacement.h5"
            with h5py.File(replacement, "w") as file:
                add_virtual(file, "data", str(outside / "secret.h5"))
            content = replacement.read_bytes()
            content += bytes(stat.st_size - len(content))
            with open(source, "r+b") as file:  # The same inode.
                file.write(content)
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            assert os.stat(source)[:9] == stat[:9]  # Only the change time differs.
        else:
            if change == "replace":
                os.remove(source)
            os.symlink(outside / "secret.h5", source)
        with pytest.raises(BrokenLink):
            read(uri, [readable])


@pytest.fixture
def client(layout):
    catalog = from_uri(
        f"sqlite:///{layout / 'catalog.db'}",
        readable_storage=[layout / "readable"],
        init_if_not_exists=True,
    )
    with Context.from_app(build_app(catalog)) as context:
        yield from_context(context)


def register(client, uri, key="master", parameters=None):
    adapter = HDF5Adapter.from_uris(uri)
    return client.new(
        key=key,
        structure_family=adapter.structure_family,
        metadata={},
        data_sources=[
            DataSource(
                structure_family=StructureFamily.container,
                mimetype="application/x-hdf5",
                structure=None,
                parameters=parameters or {},
                management=Management.external,
                assets=[Asset(data_uri=uri, is_directory=False, parameter="data_uris")],
            )
        ],
    )


@pytest.mark.parametrize("name", ["link-inside", "virtual-inside"])
def test_server_follows_reference_into_readable_storage(layout, client, name):
    register(client, build(layout, name))

    assert (client["master"]["x"].read() == INSIDE).all()


@pytest.mark.parametrize(
    "name",
    [
        "external-storage-outside",
        "link-outside",
        "virtual-outside",
        "virtual-of-relay-external-storage",
    ],
)
def test_server_refuses_reference_out_of_readable_storage(layout, client, name):
    register(client, build(layout, name))

    assert list(client["master"]) == ["x"]
    # The client reports a broken link as a missing key.
    with pytest.raises(KeyError, match="broken link"):
        client["master"]["x"]
    for route in ("metadata", "array/full"):
        response = client.context.http_client.get(
            f"/api/v1/{route}/master/x",
            headers={"Accept": "application/octet-stream"},
        )
        assert response.status_code == HTTP_410_GONE
        assert SECRET not in response.content


def test_client_cannot_widen_readable_directories(layout, client):
    "The directories come from the server's configuration, not from the node."
    register(
        client,
        build(layout, "link-outside"),
        parameters={"readable_directories": ["/"]},
    )

    response = client.context.http_client.get("/api/v1/array/full/master/x")
    assert response.status_code == HTTP_410_GONE


@pytest.fixture
def netcdf_file(layout):
    xarray = pytest.importorskip("xarray")
    path = layout / "readable" / "data.nc"
    xarray.Dataset({"t": ("x", numpy.full(3, INSIDE))}).to_netcdf(
        path, engine="h5netcdf"
    )
    # NetCDF stores user-defined types as committed datatypes.
    with h5py.File(path, "a") as file:
        file["compound_type"] = numpy.dtype([("a", "i4"), ("b", "f8")])
    return path


def register_netcdf(client, path):
    return client.new(
        key="nc",
        structure_family=StructureFamily.container,
        metadata={},
        data_sources=[
            DataSource(
                structure_family=StructureFamily.container,
                mimetype="application/x-netcdf",
                structure=None,
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(path),
                        is_directory=False,
                        parameter="data_uri",
                    )
                ],
            )
        ],
    )


def test_server_reads_netcdf_file(layout, client, netcdf_file):
    register_netcdf(client, netcdf_file)

    assert (client["nc"]["t"].read() == INSIDE).all()


@pytest.mark.parametrize("kind", ["link", "virtual", "external-storage"])
def test_server_refuses_netcdf_file_that_refers_to_another_file(
    layout, client, netcdf_file, kind
):
    "A NetCDF-4 file is an HDF5 file. It is served only if it stands alone."
    with h5py.File(netcdf_file, "a") as file:
        if kind == "link":
            file["s"] = h5py.ExternalLink(
                str(layout / "outside" / "secret.h5"), "/data"
            )
        elif kind == "virtual":
            add_virtual(file, "s", str(layout / "outside" / "secret.h5"))
        else:
            add_external_storage(file, "s", layout / "outside" / "secret.txt")
    register_netcdf(client, netcdf_file)

    # Nothing in the file is served, also not what is stored in the file itself.
    for route in ("search/nc", "metadata/nc/s", "array/full/nc/s", "array/full/nc/t"):
        response = client.context.http_client.get(f"/api/v1/{route}")
        assert response.status_code == HTTP_410_GONE
