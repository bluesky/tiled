# Compression

## Considerations

The Tiled server factors in the following considerations when
determining whether and how to compress the data before transmitting it
to the client.

* Which compression methods can the client handle? Some of the more
  efficient algorithms are not commonly understood by all clients.
  The server implements standard HTTP
  [proactive content negotiation](https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation)
  to identify compatible compression methods ("encodings").
* Is this data format already compressed? For example, PNG has its own internal
  compression built in, so additional compression would have low returns.
  Formats like these will be sent without any additional compression.
* Formats like a strided array (i.e. numpy) buffer have a natural item
  size (bit width). Certain compression method can take advantage of this
  knowledge to achieve faster compression and better compression ratios. Thus,
  they should be preferred in these situations if both the server and the
  client support them.
* Is the data so small that it's not worth compressing?
  Compression is generally more effective on larger data because the compressor
  has the opportunity to observe and take advantage of patterns in the data.
  Also, if the data is small to begin with, reducing its size contributes little
  to the overall time spent transferring the HTTP message.
* Does the data compress well? Some scientific data, such as sparse images,
  compresses very well. The time spent compressing and decompressing is
  easily made up for by the time saved in transmitting a smaller payload. But
  some scientific data, with high entropy, compresses poorly. If Tiled finds
  that data does not compress well, it just sends the uncompressed original to
  save the client the time of decompressing it.

## Supported Compression Methods

Tiled supports both common and specialized high-performance compression methods.

For broad compatibility, it supports `gzip` compression, which is
[the most common one](https://developer.mozilla.org/en-US/docs/Web/HTTP/Compression)
used in HTTP clients---supported by web browsers, command-line tools like
[curl](https://curl.se/) or [https://httpie.io/](https://httpie.io/), and
frameworks like [requests](https://docs.python-requests.org/),
[httpx](https://www.python-httpx.org/), and likely any other framework currently
maintained.

However, `gzip` is slow compared to newer alternatives. Therefore, the Tiled
server supports others if the relevant dependencies are installed. Compression
settings and availability vary by media type. In general Tiled prefers earlier
entries in this table above later ones.

| Method                                                           | Accept-Encoding | Required Python Package |
| ---------------------------------------------------------------- | --------------- | ----------------------- |
| [blosc2](https://www.blosc.org/)                                 | `blosc2`        | `blosc2`                |
| [lz4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) | `lz4`           | `lz4`                   |
| [Zstandard](https://facebook.github.io/zstd/)                    | `zstd`          | `zstandard`             |
| [gzip](https://en.wikipedia.org/wiki/Gzip)                       | `gzip`          | none (built in)         |

The Tiled Python *client* currently supports gzip, zstd, and blosc2 (as long as
the associated optional dependency is installed).

## Example Requests and Responses

In these examples we'll use the command-line HTTP client
[httpie](https://httie.io/) to show just the *headers* of HTTP
requests and responses.

By default, it requests one of the standard encodings `gzip` or `deflate`. Of
those two, the Tiled server knows `gzip`, so it uses that.

```
$ http -p Hh :8000/array/full/A
GET /array/full/A HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:8000
User-Agent: HTTPie/1.0.3

HTTP/1.1 200 OK
content-encoding: gzip
content-length: 472
content-type: application/octet-stream
date: Mon, 26 Jul 2021 01:30:19 GMT
etag: a6a4697f732308159745eab706de8463
server: uvicorn
server-timing: compress;time=0.27;ratio=169.49, app;dur=6.7
set-cookie: tiled_csrf=gGgRTzuMpENi52p-imS0YTHkdRAZcZZf1H-3RJpQHog; HttpOnly; Path=/; SameSite=lax
vary: Accept-Encoding
```

The relevant line in the request is

```
Accept-Encoding: gzip, deflate
```

where the client tells the server which compression algorithms it can decompress.

The relevant line in the response is

```
content-encoding: gzip
```

where the server tells us which, if any, compression algorithm it applied.
Also, notice the line

```
server-timing: compress;time=0.27;ratio=169.49, app;dur=6.7
```

where the server reports the compression ratio (higher is better) and the time
in milliseconds that it cost to compress it, beside other metrics.

In the next example, the server's Python environment has the Python package
`zstandard` installed. It will prefer to use the superior algorithm `zstd` if
the client lists it as one that it supports. Here, the client lists `zstd` and
`gzip`.

```
$ http -p Hh :8000/table/full/C accept-encoding:zstd,gzip
GET /table/full/C HTTP/1.1
Accept: */*
Connection: keep-alive
Host: localhost:8000
User-Agent: HTTPie/1.0.3
accept-encoding: zstd

HTTP/1.1 200 OK
content-encoding: zstd
content-length: 558
content-type: application/vnd.apache.arrow.file
date: Mon, 26 Jul 2021 01:19:05 GMT
etag: 6389586cf110bbbc5e69a329ee07e763
server: uvicorn
server-timing: compress;time=0.10;ratio=8.06 app;dur=11.0
set-cookie: tiled_csrf=iRPOSCkpotnglSpyCwwG7GSof-DzfZBNGNDG3suhj8w; HttpOnly; Path=/; SameSite=lax
vary: Accept-Encoding
```

Finally, in this example. the server decides that the raw, compressed content is
so small (304 bytes) that it isn't worth compressing.

```
$ http -p Hh :8000/metadata/
GET /metadata/ HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate
Connection: keep-alive
Host: localhost:8000
User-Agent: HTTPie/1.0.3

HTTP/1.1 200 OK
content-length: 304
content-type: application/json
date: Mon, 26 Jul 2021 01:46:23 GMT
etag: 5ab946941f733dd41b485cec8afee8c9
server: uvicorn
server-timing: app;dur=4.0
set-cookie: tiled_csrf=DqqsY-w2dWsVt7EYA53VkEk8cATz_6jINCYhvu2eEls; HttpOnly; Path=/; SameSite=lax
```

## Design Acknowledgement

Tiled's compression implementation heavily influenced by the dask module
`distributed.protocol.compression`. The important difference is that
`distributed` is in control of both the server and the client, and they communicate
over its internal custom TCP protocol. We are operating over HTTP with a mixture
of clients we control (e.g. Tiled's Python client) and clients we don't (e.g.
curl).
