import numpy as np

from ..catalog import from_uri
from ..client import Context, from_context
from ..server.app import build_app


def test_subscribe_immediately_after_creation_websockets(tmpdir):
    # Create tree without cache_settings to avoid early Redis client creation
    tree = from_uri(
        "sqlite:///:memory:",
        writable_storage=[
            f"file://localhost{str(tmpdir / 'data')}",
            f"duckdb:///{tmpdir / 'data.duckdb'}",
        ],
        readable_storage=None,
        init_if_not_exists=True,
        cache_settings={
            "uri": "redis://localhost:6379",
            "ttl": 60,
        },  # No Redis client created yet
    )

    app = build_app(
        tree,
        authentication={"single_user_api_key": "secret"},
    )

    # with Context.from_app(app, api_key="secret") as context:
    with Context.from_app(app) as context:
        client = from_context(context)
        # Create streaming array node using Tiled client
        arr = np.arange(10)
        streaming_node = client.write_array(
            arr, key="test_stream_immediate", is_streaming=True
        )

        test_client = context.http_client
        # Connect WebSocket using TestClient with msgpack format and authorization
        with test_client.websocket_connect(
            "/api/v1/stream/single/test_stream_immediate?envelope_format=msgpack",
            headers={"Authorization": "secret"},
        ) as websocket:
            # Write updates using Tiled client
            for i in range(1, 4):
                new_arr = np.arange(10) + i
                print("INSERT", new_arr)
                streaming_node.write(new_arr)

            # Receive all updates
            received = []
            print("BEFORE RECEIVE")
            for _ in range(3):
                msg_bytes = websocket.receive_bytes()
                print("RECEIVED", msg_bytes)
                # Tiled uses msgpack format
                import msgpack

                msg = msgpack.unpackb(msg_bytes)
                received.append(msg)

            # Verify all updates received in order
            assert len(received) == 3
            # Check that we received messages with the expected data
            for i, msg in enumerate(received):
                assert "timestamp" in msg
                assert "payload" in msg
                assert msg["shape"] == [10]
                # Verify payload contains the expected array data
                payload_array = np.frombuffer(msg["payload"], dtype=np.int64)
                expected_array = np.arange(10) + (i + 1)
                np.testing.assert_array_equal(payload_array, expected_array)
