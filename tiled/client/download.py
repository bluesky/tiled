"Download utilities implemented using httpx and rich progress bars, with parallelism."
import io
import re
import signal
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event, Lock
from typing import Iterable, MutableMapping, Optional, Union

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


def _resolve_placeholder(target, response: httpx.Response):
    """Substitute `ATTACHMENT_FILENAME_PLACEHOLDER` using the server-provided
    `Content-Disposition` filename. Works for both `Path` (filename position)
    and `str` (anywhere in the key). Passes through unchanged when no
    placeholder is present."""
    if isinstance(target, Path):
        if target.name != ATTACHMENT_FILENAME_PLACEHOLDER:
            return target
        filename = CONTENT_DISPOSITION_PATTERN.match(
            response.headers["Content-Disposition"]
        ).group(1)
        return Path(target.parent, filename)
    if ATTACHMENT_FILENAME_PLACEHOLDER not in target:
        return target
    filename = CONTENT_DISPOSITION_PATTERN.match(
        response.headers["Content-Disposition"]
    ).group(1)
    return target.replace(ATTACHMENT_FILENAME_PLACEHOLDER, filename)


def _download_url(
    progress: Progress,
    task_id: TaskID,
    done_event: Event,
    client: httpx.Client,
    url: str,
    target: Union[Path, str],
    mapping: Optional[MutableMapping],
    lock: Optional[Lock],
):
    """Fetch `url` and write the body to disk (when `mapping is None`) or to
    a `BytesIO` stored in `mapping` (otherwise). Returns the resolved target."""
    progress.console.log(f"Requesting {url}")
    resolved = target
    try:
        if mapping is None:
            target.parent.mkdir(exist_ok=True, parents=True)
        for attempt in retry_context():
            with attempt:
                with client.stream("GET", url) as response:
                    handle_error(response)
                    resolved = _resolve_placeholder(target, response)
                    # Content-Length is absent when the server streams a
                    # chunked response (e.g. when compression middleware
                    # re-encodes the body). Fall back to an indeterminate
                    # progress bar in that case.
                    content_length = response.headers.get("Content-Length")
                    total = int(content_length) if content_length else None
                    progress.update(task_id, total=total)
                    progress.start_task(task_id)
                    if mapping is None:
                        sink = open(resolved, "wb")
                    else:
                        sink = io.BytesIO()
                    try:
                        for chunk in response.iter_bytes():
                            sink.write(chunk)
                            progress.update(task_id, advance=len(chunk))
                            if done_event.is_set():
                                return resolved
                        if mapping is not None:
                            sink.seek(0)
                            with lock:
                                mapping[resolved] = sink
                    finally:
                        if mapping is None:
                            sink.close()
    except Exception as err:
        progress.console.log(f"ERROR {err!r}")
    else:
        progress.console.log(f"Downloaded {resolved}")
    return resolved


def download(
    client,
    urls: Iterable[str],
    targets: Iterable,
    *,
    mapping: Optional[MutableMapping] = None,
    max_workers: int = 4,
):
    """Download multiple URLs in parallel.

    When `mapping` is `None` (the default), each item in `targets` must be a
    filesystem `Path`. When `mapping` is a `MutableMapping`, each item must
    be a string key; the corresponding response body is stored as an
    `io.BytesIO` (seeked to 0) under that key.

    A target may embed `ATTACHMENT_FILENAME_PLACEHOLDER`, which is replaced
    with the filename advertised by the server via `Content-Disposition`.
    Returns the list of resolved targets in submission order.
    """
    if len(urls) != len(targets):
        kind = "keys" if mapping is not None else "paths"
        raise ValueError(
            f"Must provide a list of URLs and a list of {kind} "
            f"with equal length. Received {len(urls)=} and "
            f"len({kind})={len(targets)}."
        )

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

    def sigint_handler(signum, frame):
        done_event.set()
        original_sigint_handler(signal.SIGINT, frame)

    done_event = Event()
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)
    lock = Lock() if mapping is not None else None
    futures = []
    try:
        with progress:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for url, target in zip(urls, targets):
                    task_id = progress.add_task("download", start=False)
                    future = pool.submit(
                        _download_url,
                        progress,
                        task_id,
                        done_event,
                        client,
                        url,
                        target,
                        mapping,
                        lock,
                    )
                    futures.append(future)
                wait(futures)
    finally:
        # Restore SIGINT handler.
        signal.signal(signal.SIGINT, original_sigint_handler)
    return [future.result() for future in futures]
