import contextlib
import os
import re
import subprocess
import sys
import threading
from queue import Queue

import httpx
import pytest


@contextlib.contextmanager
def run_cli(command):
    "Run '/path/to/this/python -m ...'"
    process = subprocess.Popen(
        [sys.executable, "-m"] + command.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    yield process
    process.terminate()


def scrape_server_url_from_logs(process):
    "Scrape from server logs 'Uvicorn running on https://...'"

    def target(queue):
        pattern = re.compile(r"Uvicorn running on .*(http:\/\/\S+:\d+).*")
        lines = []
        while not process.poll():
            line = process.stderr.readline()
            lines.append(line.decode())
            if match := pattern.search(line.decode()):
                break
        else:
            raise RuntimeError(
                "Did not find server URL in log output:\n" + "\n".join(lines)
            )
        url = match.group(1)
        queue.put(url)

    queue = Queue()
    thread = threading.Thread(target=target, args=(queue,))
    thread.start()
    url = queue.get(timeout=10)
    # If the server has an error starting up, the target() will
    # never find a match, and a TimeoutError will be raised above.
    # The thread will leak. This is the best reasonably simple,
    # portable approach available.
    thread.join()
    _, port = url.rsplit(":", 1)
    assert port != "8000"  # should be a random high port
    return url


def check_server_readiness(process):
    "Given a server process, check that it responds successfully to HTTP."
    url = scrape_server_url_from_logs(process)
    httpx.get(url).raise_for_status()


@pytest.mark.parametrize(
    "args",
    [
        "",
        "--verbose",
        "--api-key secret",
    ],
)
def test_serve_directory(args, tmp_path):
    "Test 'tiled serve directory ... with a variety of arguments."
    with run_cli(f"tiled serve directory {tmp_path!s} --port 0 " + args) as process:
        check_server_readiness(process)


@pytest.mark.parametrize(
    "args",
    [
        "",
        "--api-key secret",
    ],
)
def test_serve_catalog_temp(args, tmp_path):
    "Test 'tiled serve catalog --temp ... with a variety of arguments."
    with run_cli(f"tiled serve directory {tmp_path!s} --port 0 " + args) as process:
        check_server_readiness(process)


@pytest.mark.parametrize(
    "args",
    [
        "",
    ],
)
def test_serve_config(args, tmp_path, sqlite_or_postgres_uri):
    "Test 'tiled serve config' with a tmp config file."
    (tmp_path / "data").mkdir()
    (tmp_path / "config").mkdir()
    config_filepath = tmp_path / "config" / "config.yml"
    with open(config_filepath, "w") as file:
        file.write(
            f"""
authentication:
  allow_anonymous_access: false
trees:
  - path: /
    tree: catalog
    args:
      uri: {sqlite_or_postgres_uri}
      writable_storage: {tmp_path / 'data'}
      init_if_not_exists: true
"""
        )
    with run_cli(f"tiled serve config {config_filepath} --port 0 " + args) as process:
        check_server_readiness(process)


def test_cli_version():
    from tiled import __version__

    with run_cli("tiled --version") as process:
        assert process.stdout is not None
        line = process.stdout.readline()
    assert line.decode() == f"{__version__}{os.linesep}"
