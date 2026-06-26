"Download utilities implemented using httpx and rich progress bars, with parallelism."
import io
import re
import signal
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event, Lock
from typing import Iterable, MutableMapping

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
                with client.stream("GET", url) as response:
                    handle_error(response)
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


def _download_url_to_buffer(
    progress: Progress,
    task_id: TaskID,
    done_event: Event,
    client: httpx.Client,
    url: str,
    key: str,
    mapping: MutableMapping,
    lock: Lock,
) -> str:
    """Stream a url into an in-memory buffer and store it in `mapping`.

    `key` may contain `ATTACHMENT_FILENAME_PLACEHOLDER`, in which case the
    placeholder segment is replaced with the filename advertised by the server
    via the `Content-Disposition` header. Returns the resolved key.
    """
    progress.console.log(f"Requesting {url}")
    resolved_key = key
    try:
        for attempt in retry_context():
            with attempt:
                with client.stream("GET", url) as response:
                    handle_error(response)
                    if ATTACHMENT_FILENAME_PLACEHOLDER in key:
                        filename = CONTENT_DISPOSITION_PATTERN.match(
                            response.headers["Content-Disposition"]
                        ).group(1)
                        resolved_key = key.replace(
                            ATTACHMENT_FILENAME_PLACEHOLDER, filename
                        )
                    buf = io.BytesIO()
                    total = int(response.headers["Content-Length"])
                    progress.update(task_id, total=total)
                    progress.start_task(task_id)
                    for chunk in response.iter_bytes():
                        buf.write(chunk)
                        progress.update(task_id, advance=len(chunk))
                        if done_event.is_set():
                            return resolved_key
                    buf.seek(0)
                    with lock:
                        mapping[resolved_key] = buf
    except Exception as err:
        progress.console.log(f"ERROR {err!r}")
    else:
        progress.console.log(f"Downloaded {resolved_key}")
    return resolved_key


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


def download_to_buffers(
    client,
    urls: Iterable[str],
    keys: Iterable[str],
    mapping: MutableMapping,
    *,
    max_workers: int = 4,
):
    """Download multiple URLs into in-memory buffers stored in `mapping`.

    Each downloaded payload is stored in `mapping` as an `io.BytesIO` (seeked to
    0) under the corresponding key. Keys may embed
    `ATTACHMENT_FILENAME_PLACEHOLDER`, which is substituted with the filename
    from the server's `Content-Disposition` header. Returns the list of
    resolved keys in submission order.
    """
    progress = Progress(
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    if len(urls) != len(keys):
        raise ValueError(
            "Must provide a list of URLs and a list of keys with equal length. "
            f"Received {len(urls)=} and {len(keys)=}."
        )

    def sigint_handler(signum, frame):
        done_event.set()
        original_sigint_handler(signal.SIGINT, frame)

    done_event = Event()
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)
    lock = Lock()
    futures = []
    try:
        with progress:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for url, key in zip(urls, keys):
                    task_id = progress.add_task("download", start=False)
                    future = pool.submit(
                        _download_url_to_buffer,
                        progress,
                        task_id,
                        done_event,
                        client,
                        url,
                        key,
                        mapping,
                        lock,
                    )
                    futures.append(future)
                wait(futures)
    finally:
        signal.signal(signal.SIGINT, original_sigint_handler)
    return [future.result() for future in futures]
