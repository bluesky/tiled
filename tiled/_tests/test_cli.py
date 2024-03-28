import re
import subprocess
import sys
import threading
from queue import Queue

import httpx
import pytest


def run_cli(command):
    "Run '/path/to/this/python -m ...'"
    return subprocess.Popen(
        [sys.executable, "-m"] + command.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def scrape_server_url_from_logs(process):
    "Scrape from server logs 'Uvicorn running on https://...'"

    def target(queue):
        pattern = re.compile(r"Uvicorn running on (\S*)")
        while not process.poll():
            line = process.stderr.readline()
            if match := pattern.search(line.decode()):
                break
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
    return url


def check_server_readiness(process):
    "Given a server process, check that it responds successfully to HTTP."
    url = scrape_server_url_from_logs(process)
    httpx.get(url).raise_for_status()
    process.terminate()


@pytest.mark.parametrize(
    "args",
    [
        "",
        "--verbose",
        "--api-key secret",
    ],
)
def test_serve_directory(args, tmpdir):
    "Test 'tiled serve directory ... with a variety of arguments."
    process = run_cli(f"tiled serve directory {tmpdir!s} --port 0 " + args)
    check_server_readiness(process)


@pytest.mark.parametrize(
    "args",
    [
        "",
        "--api-key secret",
    ],
)
def test_serve_catalog_temp(args, tmpdir):
    "Test 'tiled serve catalog --temp ... with a variety of arguments."
    process = run_cli(f"tiled serve directory {tmpdir!s} --port 0 " + args)
    check_server_readiness(process)
