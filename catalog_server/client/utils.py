import httpx


def handle_error(response):
    try:
        response.raise_for_status()
    except httpx.RequestError:
        raise  # Nothing to add in this case; just raise it.
    except httpx.HTTPStatusError as exc:
        if response.status_code < 500:
            # Include more detail that httpx does by default.
            message = (
                f"{exc.response.status_code}: "
                f"{exc.response.json()['detail']} "
                f"{exc.request.url}"
            )
            raise ClientError(message, exc.request, exc.response) from exc
        else:
            raise


class ClientError(httpx.HTTPStatusError):
    def __init__(self, message, request, response):
        super().__init__(message=message, request=request, response=response)
