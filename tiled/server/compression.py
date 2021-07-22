from gzip import compress
import io

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class CompressionMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        compression_registry,
        minimum_size: int = 500,
    ) -> None:
        self.app = app
        self.compression_registry = compression_registry
        self.minimum_size = minimum_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = Headers(scope=scope)
            accepted = {
                item.strip() for item in headers.get("accept-encoding").split(",")
            }
            responder = CompressionResponder(
                self.app, self.minimum_size, accepted, self.compression_registry
            )
            await responder(scope, receive, send)
            return
        await self.app(scope, receive, send)


class CompressionResponder:
    def __init__(
        self, app: ASGIApp, minimum_size: int, accepted: set, compression_registry
    ) -> None:
        self.app = app
        self.minimum_size = minimum_size
        self.accepted = accepted
        self.compression_registry = compression_registry
        self.send: Send = unattached_send
        self.initial_message: Message = {}
        self.started = False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.send = send
        await self.app(scope, receive, self.send_compressed)

    async def send_compressed(self, message: Message) -> None:
        message_type = message["type"]
        if message_type == "http.response.start":
            # Don't send the initial message until we've determined how to
            # modify the outgoing headers correctly.
            self.initial_message = message
        elif message_type == "http.response.body" and not self.started:
            self.started = True
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            if len(body) < self.minimum_size and not more_body:
                # Don't apply compression to small outgoing responses.
                await self.send(self.initial_message)
                await self.send(message)
            elif not more_body:
                headers = MutableHeaders(raw=self.initial_message["headers"])
                media_type = headers["Content-Type"]
                for encoding in self.compression_registry.encodings(media_type):
                    if encoding in self.accepted:
                        file_factory = self.compression_registry.dispatch(
                            media_type=media_type,
                            encoding=encoding,
                        )
                        break
                else:
                    # Could not negotiate a support compression for this media type
                    # Send uncompressed.
                    await self.send(self.initial_message)
                    await self.send(message)
                    return
                # Standard compressed response.
                self.compressed_buffer = io.BytesIO()
                self.compressed_file = file_factory(self.compressed_buffer)
                self.compressed_file.write(body)
                self.compressed_file.close()
                body = self.compressed_buffer.getvalue()

                headers["Content-Encoding"] = encoding
                headers["Content-Length"] = str(len(body))
                headers.add_vary_header("Accept-Encoding")
                message["body"] = body

                await self.send(self.initial_message)
                await self.send(message)
            else:
                # Initial body in streaming compressed response.
                headers = MutableHeaders(raw=self.initial_message["headers"])
                headers["Content-Encoding"] = encoding
                headers.add_vary_header("Accept-Encoding")
                del headers["Content-Length"]

                self.compressed_file.write(body)
                message["body"] = self.compressed_buffer.getvalue()
                self.compressed_buffer.seek(0)
                self.compressed_buffer.truncate()

                await self.send(self.initial_message)
                await self.send(message)

        elif message_type == "http.response.body":
            # Remaining body in streaming compressed response.
            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            self.compressed_file.write(body)
            if not more_body:
                self.compressed_file.close()

            message["body"] = self.compressed_buffer.getvalue()
            self.compressed_buffer.seek(0)
            self.compressed_buffer.truncate()

            await self.send(message)


async def unattached_send(message: Message) -> None:
    raise RuntimeError("send awaitable not set")  # pragma: no cover
