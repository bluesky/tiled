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


def test_mount_node_nonexistent(sqlite_or_postgres_uri, tmpdir):
    "A tree whose mount_node does not exist in the database should be silently excluded."
    # Initialize the catalog database with a single container "A".
    init_config = {
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
    with Context.from_app(build_app_from_config(init_config)) as context:
        client = from_context(context)
        client.create_container("A")

    # Mount two trees: one pointing at an existing node, one at a nonexistent node.
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
                    "mount_node": "/does_not_exist",
                },
            },
        ]
    }
    with Context.from_app(build_app_from_config(multi_tree_config)) as context:
        client = from_context(context)
        # Only the tree with the existing mount_node should be served.
        assert list(client) == ["a"]


def test_create_mount_nodes_if_not_exist(sqlite_or_postgres_uri, tmpdir):
    "Test that create_mount_nodes_if_not_exist auto-creates missing mount node paths."
    # Initialize the catalog database (empty, no containers).
    init_config = {
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
    with Context.from_app(build_app_from_config(init_config)) as context:
        client = from_context(context)
        # Database is empty, no containers exist.
        assert list(client) == []

    # Mount a tree at a nonexistent path with create_mount_nodes_if_not_exist=True.
    # This should auto-create intermediate container nodes /X/Y/Z.
    mount_config = {
        "create_mount_nodes_if_not_exist": True,
        "trees": [
            {
                "path": "/",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": "/X/Y/Z",
                },
            },
        ],
    }
    with Context.from_app(build_app_from_config(mount_config)) as context:
        client = from_context(context)
        # The tree should be served (mount node was auto-created).
        # We can write into it.
        client.create_container("child")
        assert list(client) == ["child"]

    # Verify the nodes were actually created by mounting the root.
    root_config = {
        "trees": [
            {
                "path": "/",
                "tree": "catalog",
                "args": {
                    "uri": sqlite_or_postgres_uri,
                    "writable_storage": [tmpdir / "data"],
                },
            },
        ]
    }
    with Context.from_app(build_app_from_config(root_config)) as context:
        client = from_context(context)
        # The auto-created path should be visible from root.
        assert "X" in list(client)
        assert "Y" in list(client["X"])
        assert "Z" in list(client["X"]["Y"])
        assert "child" in list(client["X"]["Y"]["Z"])
