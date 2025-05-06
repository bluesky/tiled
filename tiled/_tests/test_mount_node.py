from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config


def test_mount_node(tmpdir):
    "Test 'mounting' sub-trees of a catalog."
    catalog_uri = f"sqlite:///{tmpdir}/catalog.db"
    one_tree_config = {
        "trees": [
            {
                "path": "/",
                "tree": "catalog",
                "args": {
                    "uri": catalog_uri,
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
                    "uri": catalog_uri,
                    "init_if_not_exists": True,
                    "writable_storage": [tmpdir / "data"],
                    "mount_node": "/A",
                },
            },
            {
                "path": "/b",
                "tree": "catalog",
                "args": {
                    "uri": catalog_uri,
                    "init_if_not_exists": True,
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
        client["b"].create_container("y")
        # Check for no cross-talk.
        assert list(client) == ["a", "b"]
        assert list(client["a"]) == ["x"]
        assert list(client["b"]) == ["y"]
