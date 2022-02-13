import os
import stat
import typing
from email.utils import parsedate
from pathlib import Path

import anyio
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, Response
from starlette.staticfiles import NotModifiedResponse
from starlette.types import Receive, Scope, Send

from .utils import get_base_url_low_level

PathLike = typing.Union[str, "os.PathLike[str]"]


class TemplatedStaticFiles:
    """
    Enable path routing find in templates.

    - Treat the first path segment as a static file name (/node -> node.html)
    - Treat any additional path segments as path parameters and template
      them into the response (/node/a/b/c templates "a/b/c" in to node.html)
    """

    def __init__(
        self,
        *,
        directories: typing.List[PathLike] = [],
        api_url: typing.Optional[str] = None,
    ):
        self.directories = directories
        self.api_url = api_url
        self.config_checked = False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        The ASGI entry point.
        """
        assert scope["type"] == "http"

        if not self.config_checked:
            await self.check_config()
            self.config_checked = True

        path = self.get_path(scope)
        response = await self.get_response(path, scope)
        await response(scope, receive, send)

    def get_path(self, scope: Scope) -> str:
        """
        Given the ASGI scope, return the `path` string to serve up,
        with OS specific path separators, and any '..', '.' components removed.
        """
        return os.path.normpath(os.path.join(*scope["path"].split("/")))

    async def get_response(self, path: str, scope: Scope) -> Response:
        """
        Returns an HTTP response, given the incoming path, method and request headers.
        """
        if scope["method"] not in ("GET", "HEAD"):
            raise HTTPException(status_code=405)

        try:
            static_path, path_params, stat_result = await anyio.to_thread.run_sync(
                self.lookup_path, path
            )
        except PermissionError:
            raise HTTPException(status_code=401)
        except OSError:
            raise

        if stat_result and stat.S_ISREG(stat_result.st_mode):
            # We have a static file to serve.
            return self.template_response(static_path, path_params, scope)

        # Check for '404.html'.
        static_path, path_params, stat_result = await anyio.to_thread.run_sync(
            self.lookup_path, "404.html"
        )
        if stat_result and stat.S_ISREG(stat_result.st_mode):
            return FileResponse(
                static_path,
                stat_result=stat_result,
                method=scope["method"],
                status_code=404,
            )

        raise HTTPException(status_code=404)

    def lookup_path(
        self, path: str
    ) -> typing.Tuple[str, typing.List[str], typing.Optional[os.stat_result]]:
        for directory in self.directories:
            segments = Path(path).parts
            if not segments:
                segments = ["index"]
            static_path = os.path.realpath(Path(directory, f"{segments[0]}.html"))
            directory = os.path.realpath(directory)
            if os.path.commonprefix([static_path, directory]) != directory:
                # Don't allow misbehaving clients to break out of the static files
                # directory.
                continue
            try:
                return static_path, segments[1:], os.stat(static_path)
            except (FileNotFoundError, NotADirectoryError):
                continue
        return "", [], None

    def template_response(
        self,
        static_path: PathLike,
        path_params: PathLike,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        request_headers = Headers(scope=scope)
        with open(static_path, mode="rt") as file:
            if self.api_url is None:
                # Assume that we are being served alongside the API, and
                # sort out the URL from the request URL.
                # This is janky but I don't think there is another way that works
                # behind a proxy.
                api_url = f"{get_base_url_low_level(request_headers, scope)[:-7]}api/"
            else:
                api_url = self.api_url
            raw_content = file.read()
            content = raw_content.replace(
                "REQUEST_PATH_PARAMS", "/".join(path_params)
            ).replace("TILED_API_URL", api_url)
            response = Response(content, status_code=status_code)
        if self.is_not_modified(response.headers, request_headers):
            return NotModifiedResponse(response.headers)
        return response

    def is_not_modified(
        self, response_headers: Headers, request_headers: Headers
    ) -> bool:
        """
        Given the request and response headers, return `True` if an HTTP
        "Not Modified" response could be returned instead.
        """
        try:
            if_none_match = request_headers["if-none-match"]
            etag = response_headers["etag"]
            if if_none_match == etag:
                return True
        except KeyError:
            pass

        try:
            if_modified_since = parsedate(request_headers["if-modified-since"])
            last_modified = parsedate(response_headers["last-modified"])
            if (
                if_modified_since is not None
                and last_modified is not None
                and if_modified_since >= last_modified
            ):
                return True
        except KeyError:
            pass

        return False

    async def check_config(self) -> None:
        """
        Perform a one-off configuration check that the directories
        actually exist, so that we can raise loud errors rather than
        just returning 404 responses.
        """
        for directory in self.directories:
            try:
                stat_result = await anyio.to_thread.run_sync(os.stat, directory)
            except FileNotFoundError:
                raise RuntimeError(
                    f"TemplatedStaticFiles directory '{directory}' does not exist."
                )
            if not (
                stat.S_ISDIR(stat_result.st_mode) or stat.S_ISLNK(stat_result.st_mode)
            ):
                raise RuntimeError(
                    f"TemplatedStaticFiles path '{directory}' is not a directory."
                )
