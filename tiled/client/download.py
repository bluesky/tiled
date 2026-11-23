"Download utilities implemented using httpx and rich progress bars, with parallelism."

import io
import re
import signal
from _thread import LockType
from collections.abc import Iterable, MutableMapping
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path, PurePath, PurePosixPath
from threading import Event, Lock

import httpx2 as httpx
from httpx2._decoders import SUPPORTED_DECODERS
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ..utils import UnsafePath, is_safe_path_segment
from .utils import handle_error, retry_context

# This extracts the filename to the Content-Disposition header.
CONTENT_DISPOSITION_PATTERN = re.compile(r"^attachment; ?filename=\"(.*)\"$")

# Accept-Encoding advertised when downloading raw assets *with* compression.
# We deliberately omit "blosc2": its client decoder buffers the entire response
# in memory and only emits it on flush(), which defeats streaming (the progress
# bar cannot advance, and a large asset is held wholly in RAM). The remaining
# decoders (zstd, gzip, ...) decompress incrementally, so downloads stream and
# report progress. "identity" is used instead when compression is disabled.
STREAMING_ACCEPT_ENCODING = ", ".join(
    encoding for encoding in SUPPORTED_DECODERS if encoding != "blosc2"
)

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
        return Path(target.parent, _attachment_filename(response))
    if ATTACHMENT_FILENAME_PLACEHOLDER not in target:
        return target
    return target.replace(
        ATTACHMENT_FILENAME_PLACEHOLDER, _attachment_filename(response)
    )


def _attachment_filename(response: httpx.Response) -> str:
    """Return the filename that the server gave in `Content-Disposition`.

    The server names the file. It does not choose where the file goes, so a
    name that is not a plain file name is refused."""
    filename = CONTENT_DISPOSITION_PATTERN.match(
        response.headers["Content-Disposition"]
    ).group(1)
    if not is_safe_path_segment(filename) or PurePath(filename).name != filename:
        raise UnsafePath(
            f"The server named the downloaded file {filename!r}, which is not "
            "a plain file name."
        )
    return filename


def require_relative_path(path: str) -> None:
    """Raise UnsafePath unless a path given by the server stays inside the
    directory that it will be joined to.

    This looks at the text of the path only. It is for the paths in an asset
    manifest, which become the paths of downloaded files."""
    # The server gives POSIX-style paths. Also read the path as this platform
    # would, which on Windows recognizes drives and backslashes.
    for flavor in {PurePosixPath, type(PurePath())}:
        parsed = flavor(path)
        # The anchor is the root or drive of an absolute path.
        if not parsed.parts or parsed.anchor or ".." in parsed.parts:
            raise UnsafePath(
                f"The server listed the file {path!r}, which is not a path "
                "inside of the asset. Refusing to download it."
            )


def _download_url(
    progress: Progress,
    task_id: TaskID,
    done_event: Event,
    client: httpx.Client,
    url: str,
    target: Path | str,
    mapping: MutableMapping | None,
    lock: LockType | None,
    accept_encoding: str | None,
):
    """Fetch `url` and write the body to disk (when `mapping is None`) or to
    a `BytesIO` stored in `mapping` (otherwise). Returns the resolved target.

    If the download fails, the error is raised, and a file that was written
    in part is removed."""
    progress.console.log(f"Requesting {url}")
    resolved = target
    written: Path | None = None
    # Override the client's default Accept-Encoding so that raw-asset downloads
    # do not negotiate blosc2 (see STREAMING_ACCEPT_ENCODING) or, when the
    # caller disables compression, are served uncompressed with a
    # Content-Length (and thus a determinate progress bar).
    headers = {"Accept-Encoding": accept_encoding} if accept_encoding else None
    try:
        if mapping is None:
            target.parent.mkdir(exist_ok=True, parents=True)
        for attempt in retry_context():
            with attempt:
                with client.stream("GET", url, headers=headers) as response:
                    if response.is_error:
                        # The body holds the server's explanation.
                        response.read()
                    handle_error(response)
                    resolved = _resolve_placeholder(target, response)
                    # Content-Length is absent when the server streams a
                    # compressed (chunked) response. In that case keep the
                    # task's pre-seeded total (the asset size supplied by the
                    # caller, or None for an indeterminate bar).
                    content_length = response.headers.get("Content-Length")
                    if content_length is not None:
                        progress.update(task_id, total=int(content_length))
                    progress.start_task(task_id)
                    if mapping is None:
                        sink = open(resolved, "wb")
                        written = resolved
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
        if written is not None:
            # Do not leave part of a file behind as if it were the whole.
            written.unlink(missing_ok=True)
        raise
    progress.console.log(f"Downloaded {resolved}")
    return resolved


def download(
    client,
    urls: Iterable[str],
    targets: Iterable,
    *,
    mapping: MutableMapping | None = None,
    max_workers: int = 4,
    totals: Iterable[int | None] | None = None,
    accept_encoding: str | None = None,
):
    """Download multiple URLs in parallel.

    When `mapping` is `None` (the default), each item in `targets` must be a
    filesystem `Path`. When `mapping` is a `MutableMapping`, each item must
    be a string key; the corresponding response body is stored as an
    `io.BytesIO` (seeked to 0) under that key.

    A target may embed `ATTACHMENT_FILENAME_PLACEHOLDER`, which is replaced
    with the filename advertised by the server via `Content-Disposition`.
    Returns the list of resolved targets in submission order.

    Every download is attempted. If any of them fails, the first error is
    raised once all are done, with a note of how many failed.

    `totals`, if given, must be parallel to `urls`; each entry pre-seeds the
    known content length so the progress bar is determinate even when the
    server omits `Content-Length` (e.g. a compressed streaming response). Use
    `None` for an entry whose size is unknown (an indeterminate bar).

    `accept_encoding`, if given, overrides the `Accept-Encoding` header for
    every request (e.g. `STREAMING_ACCEPT_ENCODING` to keep compression while
    excluding blosc2, or `"identity"` to disable compression).
    """
    if len(urls) != len(targets):
        kind = "keys" if mapping is not None else "paths"
        raise ValueError(
            f"Must provide a list of URLs and a list of {kind} "
            f"with equal length. Received {len(urls)=} and "
            f"len({kind})={len(targets)}."
        )
    if totals is None:
        totals = [None] * len(urls)
    elif len(totals) != len(urls):
        raise ValueError(
            f"Must provide as many totals as URLs. "
            f"Received {len(urls)=} and {len(totals)=}."
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
                for url, target, total in zip(urls, targets, totals):
                    task_id = progress.add_task("download", start=False, total=total)
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
                        accept_encoding,
                    )
                    futures.append(future)
                wait(futures)
    finally:
        # Restore SIGINT handler.
        signal.signal(signal.SIGINT, original_sigint_handler)
    errors = [error for future in futures if (error := future.exception())]
    if errors:
        errors[0].add_note(
            f"{len(errors)} of {len(futures)} downloads failed. "
            "This is the first error."
        )
        raise errors[0]
    return [future.result() for future in futures]
