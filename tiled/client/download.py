"Download utilities implemented using httpx and rich progress bars, with parallelism."
import re
import signal
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

CONTENT_DISPOSITION_PATTERN = re.compile(r"^attachment; ?filename=\"(.*)\"$")
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
        with client.stream("GET", url) as response:
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


def download(
    client, urls: Iterable[str], paths: Iterable[Path], *, max_workers: int = 4
):
    """Download multiple URLs to the given directory."""
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
            "Must provide a list of URLs and a list of paths with equal length"
        )

    def handle_sigint(signum, frame):
        done_event.set()

    done_event = Event()
    signal.signal(signal.SIGINT, handle_sigint)

    with progress:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for url, path in zip(urls, paths):
                task_id = progress.add_task("download", start=False)
                pool.submit(
                    _download_url, progress, task_id, done_event, client, url, path
                )
