import json
import logging
import os
import threading
import time
from urllib.parse import urlparse

import msgpack
import numpy as np
import websocket

from locust import HttpUser, between, events, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--api-key",
        type=str,
        default="secret",
        help="API key for Tiled authentication (default: secret)",
    )
    parser.add_argument(
        "--node-name",
        type=str,
        required=True,
        help="Node name for streaming test (required)",
    )


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if environment.host is None:
        raise ValueError(
            "Host must be specified with --host argument, or through the web-ui."
        )

    # Create the streaming node once for all users
    create_streaming_node(
        environment.host,
        environment.parsed_options.api_key,
        environment.parsed_options.node_name,
    )


def create_streaming_node(host, api_key, node_name):
    """Create a streaming array node using Tiled client"""
    from tiled.client import from_uri

    # Connect to Tiled server using client
    client = from_uri(host, api_key=api_key)

    # Create initial streaming array
    arr = np.full(5, 0.0, dtype=np.float64)  # Initial array with zeros
    client.write_array(arr, key=node_name)

    logger.info(f"Created streaming node: {node_name}")
    client.logout()


class WriterUser(HttpUser):
    """User that writes streaming data to a Tiled node"""

    wait_time = between(0.1, 0.2)  # Wait 0.1-0.2 seconds between writes
    weight = int(os.getenv("WRITER_WEIGHT", 1))

    def on_start(self):
        """Initialize user state"""
        self.node_name = self.environment.parsed_options.node_name
        self.message_count = 0
        self.api_key = self.environment.parsed_options.api_key

        # Set authentication header
        self.client.headers.update({"Authorization": f"Apikey {self.api_key}"})

    @task(10)  # Run 10x as often as cleanup
    def write_data(self):
        """Write streaming data to the node"""
        # Create data with current timestamp as all values
        current_time = time.time()
        data = np.full(5, current_time, dtype=np.float64)
        binary_data = data.tobytes()

        # Post data to the streaming endpoint
        response = self.client.put(
            f"/api/v1/array/full/{self.node_name}",
            data=binary_data,
            headers={"Content-Type": "application/octet-stream"},
        )

        # Log status
        if response.status_code == 200:
            logger.debug(f"Wrote message {self.message_count} to node {self.node_name}")
            self.message_count += 1
        else:
            logger.error(
                f"Failed to write message {self.message_count}: {response.status_code} - {response.text}"
            )

    @task(1)
    def cleanup(self):
        """Periodically cleanup the stream"""
        if self.message_count > 50:
            # Close the stream
            response = self.client.delete(f"/api/v1/stream/close/{self.node_name}")
            if response.status_code == 200:
                logger.info(f"Closed stream for node {self.node_name}")

            # Reset message count (node persists for other users)
            self.message_count = 0


class StreamingUser(HttpUser):
    """User that connects to websocket stream and measures latency"""

    wait_time = between(1, 2)
    weight = int(os.getenv("STREAMING_WEIGHT", 1))

    def on_start(self):
        """Connect to the streaming endpoint"""
        self.node_name = self.environment.parsed_options.node_name
        self.api_key = self.environment.parsed_options.api_key
        self.envelope_format = "msgpack"  # Use msgpack for efficiency
        self.ws = None
        self.connected = False

        # Set up authentication for HTTP requests
        self.client.headers.update({"Authorization": f"Apikey {self.api_key}"})

        self._connect_websocket()

    def _connect_websocket(self):
        """Connect to the websocket stream"""
        try:
            # Parse host to get websocket URL
            parsed = urlparse(self.host)
            ws_scheme = "wss" if parsed.scheme == "https" else "ws"
            host = f"{ws_scheme}://{parsed.netloc}"

            ws_url = f"{host}/api/v1/stream/single/{self.node_name}?envelope_format={self.envelope_format}&start=0"

            # Create websocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                header=[
                    f"Authorization: Apikey {self.api_key}"
                ],  # Proper Apikey format for websockets
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            # Start websocket in background thread
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()

            # Wait a bit for connection
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"Failed to connect websocket: {e}")

    def _on_open(self, ws):
        """Websocket connection opened"""
        self.connected = True
        logger.info(f"WebSocket connected to {self.node_name}")

    def _on_message(self, ws, message):
        """Process websocket messages and measure latency"""
        try:
            received_time = time.time()

            if isinstance(message, bytes):
                data = msgpack.unpackb(message)
            else:
                data = json.loads(message)

            # Extract timestamp from the payload (first element of the array)
            payload = data.get("payload")
            if payload and len(payload) > 0:
                # Convert bytes back to numpy array to get the timestamp
                payload_array = np.frombuffer(payload, dtype=np.float64)
                if len(payload_array) > 0:
                    write_time = payload_array[0]
                    latency_ms = (received_time - write_time) * 1000

                    logger.debug(
                        f"WS latency (sequence {data.get('sequence', 'N/A')}): {latency_ms:.1f}ms"
                    )

                    # Report to Locust
                    events.request.fire(
                        request_type="WS",
                        name="write_to_websocket_delivery",
                        response_time=latency_ms,
                        response_length=len(message),
                        exception=None,
                    )

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            events.request.fire(
                request_type="WS",
                name="write_to_websocket_delivery",
                response_time=0,
                response_length=0,
                exception=e,
            )

    def _on_error(self, ws, error):
        """Websocket error occurred"""
        logger.error(f"WebSocket error: {error}")
        self.connected = False

    def _on_close(self, ws, close_status_code, close_msg):
        """Websocket connection closed"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False

    @task
    def keep_alive(self):
        """Dummy task to keep the user active while listening for messages"""
        if not self.connected and self.ws:
            # Try to reconnect if disconnected
            logger.info("Attempting to reconnect WebSocket...")
            self._connect_websocket()

    def on_stop(self):
        """Clean up websocket connection"""
        if self.ws:
            self.ws.close()
            logger.info("WebSocket connection closed")
