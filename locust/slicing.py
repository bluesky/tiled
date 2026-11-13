"""
random-ROI (region of interest) acceptance test for Tiled

needs no pre-registered data and no placeholders: on startup it writes one seeded
3d stack into a test container, then reads random regions of it at doubling sizes,
plus whole frames. Runs against a throwaway server:

    uv run tiled serve catalog --host 0.0.0.0 --port 8000 --api-key secret --temp --init

then, from the locust/ folder

    uv run locust -f slicing.py --host http://localhost:8000 \
        --headless -u 20 -r 2 -t 3m --only-summary

The run fails (exit code 1) if more than --max-failure-ratio of all requests fail, so
it can gate a nightly CI job or a release. Plain locust exits 1 on any single failed
request, which would make a nightly job fail on one stray network hiccup; the threshold
tolerates that while still catching a broken server.
"""

import logging
import random

import numpy as np

from locust import HttpUser, between, events, task
from tiled.client import from_uri

# frame edge of the generated stack in pixels; also bounds the random ROI offsets, so
# it must be at least the biggest SIZE. 2880 is the frame size of the PETRA III (DESY)
# detector the size ladder was first measured on
FRAME_EDGE = 2880

# frames in the generated stack - two are enough for the random frame draw to matter,
# and keep the upload at startup small (2 x 33 MB)
N_FRAMES = 2
STACK_KEY = "stack"

# ROI edge lengths in pixels - doubling from 10, so they sit evenly on a log axis
SIZES = [10, 20, 40, 80, 160, 320, 640, 1280]


@events.init_command_line_parser.add_listener
def _(parser) -> None:
    parser.add_argument(
        "--api-key",
        type=str,
        default="secret",
        help="API key for Tiled authentication (default: secret)",
    )
    parser.add_argument(
        "--container-name",
        type=str,
        default="locust_slicing",
        help="Container the test stack is written to (default: locust_slicing)",
    )
    parser.add_argument(
        "--max-failure-ratio",
        type=float,
        default=0.01,
        help="Fail the run above this fraction of failed requests (default: 0.01)",
    )


# event listener: locust hook that runs once when the run starts, before any user
# is spawned. environment - locust's shared object across all users
@events.init.add_listener
def create_test_stack(environment, **kwargs) -> None:
    """Write one seeded 3d stack once, then share its location with all users

    Fixed seed, so the bytes are identical on every run and every machine. Random
    values, so nothing compresses and every requested byte really has to be read
    """
    if environment.host is None:
        raise ValueError("Host must be specified with --host")

    options = environment.parsed_options
    root = from_uri(environment.host, api_key=options.api_key)
    if options.container_name not in root:
        root.create_container(options.container_name)
    container = root[options.container_name]

    if STACK_KEY not in container:
        # rng = random number generator
        rng = np.random.default_rng(0)
        shape = (N_FRAMES, FRAME_EDGE, FRAME_EDGE)
        stack = rng.integers(0, 2**32, shape, dtype="uint32")
        container.write_array(stack, key=STACK_KEY)

    environment.container = options.container_name
    environment.stacks = [(STACK_KEY, N_FRAMES)]
    root.logout()


@events.quitting.add_listener
def check_failure_ratio(environment, **kwargs) -> None:
    """Turn the run into pass/fail with a tolerance

    locust exits 1 on any failed request by default, so a single stray failure
    would fail a nightly run; this replaces that with a threshold. Crashed
    tasks (Python exceptions) still fail the run through locust's own rule
    """
    ratio = environment.stats.total.fail_ratio
    limit = environment.parsed_options.max_failure_ratio
    if ratio > limit:
        logging.error(f"Failure ratio {ratio:.2%} is above the limit of {limit:.2%}")
        environment.process_exit_code = 1
    elif not environment.runner.exceptions:
        # below the limit and no crashed tasks: pass, overriding "any failure = 1"
        environment.process_exit_code = 0


def slice_url(
    container: str, name: str, frame: int, row: int, column: int, size: int
) -> str:
    """Build the array URL for a SIZE x SIZE region of one frame

    Frame index is pinned as the first slice term, otherwise a 2-term slice would
    run along the frame axis and return far more bytes than the size suggests
    """
    return (
        f"/api/v1/array/full/{container}/{name}"
        f"?slice={frame},{row}:{row + size},{column}:{column + size}"
    )


def make_slice_task(size: int):
    """Factory function, so each generated task is forced to use its own size

    locust tasks have to be class methods, so a loop inside one task would show up as
    a single entry; one generated task per size gives one row per size in the stats

    Expects the user to carry `self.container` and `self.stacks`
    """

    @task
    def read_random_slice(self) -> None:
        """Fetch a random SIZE x SIZE region from a random frame of a random stack

        Random position, so consecutive requests rarely ask for the same bytes.
        The stack is small enough to sit in the server's page cache, so this tests
        that slicing works under load, not how fast the storage is

        Subtracting size from the row/column draw keeps the region inside the
        frame, so every request is exactly size x size
        """
        name, n_frames = random.choice(self.stacks)
        frame = random.randrange(n_frames)
        rdm_row = random.randint(0, FRAME_EDGE - size)
        rdm_column = random.randint(0, FRAME_EDGE - size)
        self.client.get(
            slice_url(self.container, name, frame, rdm_row, rdm_column, size),
            name=f"slice {size}x{size}",
        )

    return read_random_slice


class TiledSlicingUser(HttpUser):
    """
    Picks a random frame of the test stack and a random slice per request

    Grabs the stack location off 'environment' from `create_test_stack`
    """

    wait_time = between(1, 3)

    def on_start(self) -> None:
        api_key = self.environment.parsed_options.api_key
        self.client.headers["Authorization"] = f"apikey {api_key}"
        self.container = self.environment.container
        self.stacks = self.environment.stacks

    for _size in SIZES:
        locals()[f"read_slice_{_size}"] = make_slice_task(_size)

    @task
    def read_full_array(self) -> None:
        """Fetch one whole frame - the largest single-request payload

        The comparison point for the slices: what a user would have to download
        without server-side slicing
        """
        name, n_frames = random.choice(self.stacks)
        frame = random.randrange(n_frames)
        self.client.get(
            f"/api/v1/array/full/{self.container}/{name}?slice={frame}",
            name="full array",
        )
