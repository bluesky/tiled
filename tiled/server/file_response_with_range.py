# This is a variation on starlette's FileResponse that adds support for the
# 'Range' HTTP header.

# It is adapted from a closed PR in starlette which was reviewed by a core
# starlette maintainer but put aside for now in favor of other priorities in
# starlette development. Thus, we implement it here in tiled. If in the future
# starlette adds support upstream, we should consider refactoring to use that.

# Ref: https://github.com/encode/starlette/pull/1999
import hashlib
import os
import stat
import typing

import anyio
from starlette.responses import FileResponse, Receive, Scope, Send, formatdate
from starlette.status import HTTP_200_OK, HTTP_206_PARTIAL_CONTENT


class FileResponseWithRange(FileResponse):
    def __init__(
        self,
        path: typing.Union[str, "os.PathLike[str]"],
        status_code: int = HTTP_200_OK,
        *args,
        range: typing.Optional[typing.Tuple[int, int]] = None,
        **kwargs,
    ):
        if (range is not None) and (status_code != HTTP_206_PARTIAL_CONTENT):
            raise RuntimeError(
                f"Range requests must have a {HTTP_206_PARTIAL_CONTENT} status code."
            )
        self.range = range
        super().__init__(path, status_code, *args, **kwargs)

    def set_stat_headers(self, stat_result: os.stat_result) -> None:
        content_length = str(stat_result.st_size)
        size = str(stat_result.st_size)
        last_modified = formatdate(stat_result.st_mtime, usegmt=True)
        etag_base = str(stat_result.st_mtime) + "-" + str(stat_result.st_size)
        if self.range is not None:
            start, end = self.range
            etag_base += f"-{start}/{end}"
            content_length = str(end - start + 1)
            self.headers.setdefault("accept-ranges", "bytes")
            self.headers.setdefault("content-range", f"bytes {start}-{end}/{size}")
        else:
            content_length = size
        etag = hashlib.md5(etag_base.encode(), usedforsecurity=False).hexdigest()

        self.headers.setdefault("content-length", content_length)
        self.headers.setdefault("last-modified", last_modified)
        self.headers.setdefault("etag", etag)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.stat_result is None:
            try:
                stat_result = await anyio.to_thread.run_sync(os.stat, self.path)
                self.set_stat_headers(stat_result)
            except FileNotFoundError:
                raise RuntimeError(f"File at path {self.path} does not exist.")
            else:
                mode = stat_result.st_mode
                if not stat.S_ISREG(mode):
                    raise RuntimeError(f"File at path {self.path} is not a file.")
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        if scope["method"].upper() == "HEAD":
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        elif "extensions" in scope and "http.response.pathsend" in scope["extensions"]:
            await send({"type": "http.response.pathsend", "path": str(self.path)})
        else:
            async with await anyio.open_file(self.path, mode="rb") as file:
                if self.range is not None:
                    start, end = self.range
                    await file.seek(start)
                else:
                    start, end = 0, stat_result.st_size - 1
                remaining_bytes = end - start + 1
                more_body = True
                while more_body:
                    chunk_size = min(remaining_bytes, self.chunk_size)
                    chunk = await file.read(chunk_size)
                    remaining_bytes -= len(chunk)
                    more_body = remaining_bytes > 0 and len(chunk) == chunk_size
                    await send(
                        {
                            "type": "http.response.body",
                            "body": chunk,
                            "more_body": more_body,
                        }
                    )
        if self.background is not None:
            await self.background()
