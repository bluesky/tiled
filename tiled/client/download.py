"Download utilities implemented using httpx and rich progress bars, with parallelism."
import re
import signal
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event
from typing import Iterable

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .utils import handle_error, retry_context

# This extracts the filename to the Content-Disposition header.
CONTENT_DISPOSITION_PATTERN = re.compile(r"^attachment; ?filename=\"(.*)\"$")

# This is used by the caller of download(...) as a placeholder
# that should be substituted with the filename provided by the
# server in the Content-Disposition header.
ATTACHMENT_FILENAME_PLACEHOLDER = "CONTENT_DISPOSITION_HEADER_ATTACHMENT_FILENAME"


def _download_url(
    progress: Progress,
    task_id: TaskID,
    done_event: Event,
    client: httpx.Client,
    url: str,
    path: str,
) -> None:
    """Copy data from a url to a local file."""
    progress.console.log(f"Requesting {url}")
    try:
        path.parent.mkdir(exist_ok=True, parents=True)
        for attempt in retry_context():
            with attempt:
                with handle_error(client.stream("GET", url)) as response:
                    if path.name == ATTACHMENT_FILENAME_PLACEHOLDER:
                        # Use filename provided by server.
                        filename = CONTENT_DISPOSITION_PATTERN.match(
                            response.headers["Content-Disposition"]
                        ).group(1)
                        path = Path(path.parent, filename)
                    with open(path, "wb") as file:
                        total = int(response.headers["Content-Length"])
                        progress.update(task_id, total=total)
                        progress.start_task(task_id)
                        for chunk in response.iter_bytes():
                            file.write(chunk)
                            progress.update(task_id, advance=len(chunk))
                            if done_event.is_set():
                                return
    except Exception as err:
        progress.console.log(f"ERROR {err!r}")
    else:
        progress.console.log(f"Downloaded {path}")
    return path


def download(
    client, urls: Iterable[str], paths: Iterable[Path], *, max_workers: int = 4
):
    """
    Download multiple URLs to given paths, in parallel.
    """
    progress = Progress(
        # TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    if len(urls) != len(paths):
        raise ValueError(
            "Must provide a list of URLs and a list of paths with equal length. "
            f"Received {len(urls)=} and {len(paths)=}."
        )

    def sigint_handler(signum, frame):
        done_event.set()
        original_sigint_handler(signal.SIGINT, frame)

    done_event = Event()
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)
    futures = []
    try:
        with progress:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for url, path in zip(urls, paths):
                    task_id = progress.add_task("download", start=False)
                    future = pool.submit(
                        _download_url, progress, task_id, done_event, client, url, path
                    )
                    futures.append(future)
                wait(futures)
    finally:
        # Restore SIGINT handler.
        signal.signal(signal.SIGINT, original_sigint_handler)
    return [future.result() for future in futures]
