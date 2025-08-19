import numpy as np
from fastapi.testclient import TestClient

from ..catalog import from_uri
from ..client import Context, from_context
from ..server.app import build_app

config = {
    # "authentication": {"single_user_api_key": "secret"},
    "trees": [
        {
            "path": "/",
            "tree": "tiled.catalog:from_uri",
            "args": {
                "uri": "sqlite:///:memory:",
                "init_if_not_exists": True,
            },
        }
    ],
    "cache_settings": {"uri": "redis://localhost:6379", "ttl": 60},
}


def test_subscribe_immediately_after_creation_websockets(tmpdir):
    cache_settings = {"uri": "redis://localhost:6379", "ttl": 60}

    tree = from_uri(
        "sqlite:///:memory:",
        writable_storage=[
            f"file://localhost{str(tmpdir / 'data')}",
            f"duckdb:///{tmpdir / 'data.duckdb'}",
        ],
        readable_storage=None,
        init_if_not_exists=True,
        cache_settings=cache_settings,
    )
    app = build_app(
        tree,
        authentication={"single_user_api_key": "secret"},
    )

    test_client = TestClient(app)

    # with Context.from_app(app, api_key="secret") as context:
    with Context.from_app(app) as context:
        client = from_context(context)
        # Create streaming array node using Tiled client
        arr = np.arange(10)
        streaming_node = client.write_array(
            arr, key="test_stream_immediate", is_streaming=True
        )
        print(streaming_node)

        # Connect WebSocket using TestClient with msgpack format and authorization
        with test_client.websocket_connect(
            "/api/v1/stream/single/test_stream_immediate?envelope_format=msgpack",
            headers={"Authorization": "secret"},
        ) as websocket:
            # Write updates using Tiled client
            for i in range(1, 4):
                new_arr = np.arange(10) + i
                streaming_node.write(new_arr)

            # Receive all updates
            received = []
            for _ in range(3):
                msg_bytes = websocket.receive_bytes()
                # Tiled uses msgpack format
                import msgpack

                msg = msgpack.unpackb(msg_bytes)
                received.append(msg)

            # Verify all updates received in order
            assert len(received) == 3
            for i, msg in enumerate(received):
                assert msg["seq_num"] == i + 1
