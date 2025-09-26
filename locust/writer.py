from pathlib import Path

from bluesky import RunEngine
from bluesky.callbacks.tiled_writer import TiledWriter
from bluesky.plans import count
from ophyd.sim import det, hw

from locust import User, between, events, task
from tiled.client import from_uri


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--api-key",
        type=str,
        default="secret",
        help="API key for Tiled authentication (default: secret)",
    )
    parser.add_argument(
        "--container-name",
        type=str,
        default="locust_testing",
        help="Container name for test data (default: locust_testing)",
    )


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if environment.host is None:
        raise ValueError(
            "Host must be specified with --host argument, or through the web-UI."
        )

    environment.container_name = environment.parsed_options.container_name

    # Connect once to create the container
    environment.admin_client = from_uri(
        environment.host, api_key=environment.parsed_options.api_key
    )
    if environment.container_name not in environment.admin_client:
        environment.admin_client.create_container(environment.container_name)


class WritingUser(User):
    """
    User that writes new runs to Tiled using Bluesky RunEngine + TiledWriter.
    Uses the Python client; we do NOT hit explicit HTTP endpoints ourselves.
    """

    wait_time = between(0.5, 2)

    def on_start(self):
        # Generate an API key for this user
        api_key = self.environment.admin_client.create_api_key()["secret"]

        # Each user gets its own RE and TiledWriter
        self.tiled_client = from_uri(self.environment.host, api_key=api_key)
        self.RE = RunEngine()
        self.tw = TiledWriter(self.tiled_client[self.environment.container_name])
        self.RE.subscribe(self.tw)

    @task(1)
    def internal_data_collection(self):
        """
        Simulate a short internal data acquisition.
        """
        (uid,) = self.RE(count([det], num=10))

    @task(1)
    def external_data_collection(self):
        """
        Simulate acquisition of externally-stored data (images on disk).
        """
        save_path = Path("./sandbox/storage/external")
        save_path.mkdir(parents=True, exist_ok=True)
        img_det = hw(save_path=save_path).img
        (uid,) = self.RE(count([img_det], num=10))
