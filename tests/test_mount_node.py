import numpy

from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config


def test_mount_node(sqlite_or_postgres_uri, tmpdir):
    "Test 'mounting' sub-trees of a catalog."
    one_tree_config = {
        "trees": [
            {
                "path": "/",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "init_if_not_exists": True,
                    "writable_storage": [tmpdir / "data"],
                },
            },
        ]
    }
    # Make two containers in the catalog database, A and B.
    with Context.from_app(build_app_from_config(one_tree_config)) as context:
        client = from_context(context)
        client.create_container("A")
        client.create_container("B")

    # 'Mount' the two container nodes A and B at /a and /b respectively.
    # Now it is as if we have two separate catalogs, but they happen
    # to be housed in one database.
    multi_tree_config = {
        "trees": [
            {
                "path": "/a",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": "/A",
                },
            },
            {
                "path": "/b",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": "/B",
                },
            },
        ]
    }
    with Context.from_app(build_app_from_config(multi_tree_config)) as context:
        client = from_context(context)
        # Create a new node in each section of the catalog.
        client["a"].create_container("x")
        client["a"]["x"].create_container("i")
        client["b"].create_container("y")
        # Check for no cross-talk.
        assert list(client) == ["a", "b"]
        assert list(client["a"]) == ["x"]
        assert list(client["a"]["x"]) == ["i"]
        assert list(client["b"]) == ["y"]
        # Check that assets do not collide.
        arr1 = numpy.array([1, 2, 3])
        arr2 = numpy.array([4, 5, 6])
        ac1 = client["a"].write_array(arr1, key="c")
        ac2 = client["b"].write_array(arr2, key="c")
        assert numpy.array_equal(ac1[:], arr1)
        assert numpy.array_equal(ac2[:], arr2)
        uri1 = ac1.data_sources()[0].assets[0].data_uri
        uri2 = ac2.data_sources()[0].assets[0].data_uri
        assert uri1 != uri2

    # Mount a more deeply nested node at a nested path.
    multi_tree_config = {
        "trees": [
            {
                "path": "/some/nested/path",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": "/A/x",
                },
            },
        ]
    }
    with Context.from_app(build_app_from_config(multi_tree_config)) as context:
        client = from_context(context)
        assert list(client["some"]["nested"]["path"]) == ["i"]

    # As above, but specify the mount_node as a list of path segments.
    multi_tree_config = {
        "trees": [
            {
                "path": "/some/nested/path",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": ["A", "x"],
                },
            },
        ]
    }
    with Context.from_app(build_app_from_config(multi_tree_config)) as context:
        client = from_context(context)
        assert list(client["some"]["nested"]["path"]) == ["i"]
