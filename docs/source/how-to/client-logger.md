# Use Performance and Debug Logging

The client logs all network traffic (requests sent, responses received) and
interactions with its cache, if present. This can be especially useful for
measuring speed and identifying bottlenecks.

## Turn on client logging

```py
from tiled.client import show_logs

show_logs()
```

```{note}

This uses the Python standard library's logging framework. The `show_logs()`
function is just a convenience function that does some simple logging
configuration. It will not affect the logging behavior of any other parts of
your program; it does not alter any global logging configuration.

```

Requests (`->`) and responses (`<-`) will now be logged to the console, like so.

```py
>>> c = from_uri("https://tiled-demo.blueskyproject.io")
16:49:22.307 -> GET 'https://tiled-demo.blueskyproject.io/?root_path=true' 'host:tiled-demo.blueskyproject.io' 'accept:*/*' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49'
16:49:22.486 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:49:22 GMT content-type:application/json content-length:761 connection:keep-alive etag:35b70c6412c39db8b7b5132ddf61973c expires:Tue, 01 Feb 2022 21:59:22 GMT content-encoding:gzip vary:Accept-Encoding server-timing:tok;dur=0.1, pack;dur=0.0, compress;dur=0.1;ratio=3.1, app;dur=3.9 set-cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM; HttpOnly; Path=/; SameSite=lax
16:49:22.492 -> GET 'https://tiled-demo.blueskyproject.io/?root_path=true' 'host:tiled-demo.blueskyproject.io' 'accept:application/x-msgpack' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49' 'cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM'
16:49:22.531 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:49:22 GMT content-type:application/x-msgpack content-length:773 connection:keep-alive etag:35b70c6412c39db8b7b5132ddf61973c expires:Tue, 01 Feb 2022 21:59:22 GMT content-encoding:gzip vary:Accept-Encoding server-timing:tok;dur=0.1, pack;dur=0.0, compress;dur=0.1;ratio=2.7, app;dur=4.5
16:49:22.535 -> GET 'https://tiled-demo.blueskyproject.io//metadata/' 'host:tiled-demo.blueskyproject.io' 'accept:application/x-msgpack' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49' 'cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM'
16:49:22.572 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:49:22 GMT content-type:application/x-msgpack content-length:292 connection:keep-alive etag:821dd2a8b431ecd016f94cacd44af74f server-timing:tok;dur=0.0, pack;dur=0.0, app;dur=3.8

>>> t = c['generated']['short_table']
16:58:22.589 -> GET 'https://tiled-demo.blueskyproject.io/search/?filter%5Blookup%5D%5Bcondition%5D%5Bkey%5D=generated&sort=' 'host:tiled-demo.blueskyproject.io' 'accept:application/x-msgpack' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49.post0.dev0+g6dd1e5f' 'cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM'
16:58:22.635 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:58:22 GMT content-type:application/x-msgpack content-length:502 connection:keep-alive etag:53a7b8a84ec504259a8c29903a25ade0 server-timing:tok;dur=0.0, pack;dur=0.0, app;dur=5.9
16:58:22.638 -> GET 'https://tiled-demo.blueskyproject.io/search/generated?filter%5Blookup%5D%5Bcondition%5D%5Bkey%5D=short_table&sort=' 'host:tiled-demo.blueskyproject.io' 'accept:application/x-msgpack' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49.post0.dev0+g6dd1e5f' 'cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM'
16:58:22.681 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:58:22 GMT content-type:application/x-msgpack content-length:944 connection:keep-alive etag:8d81b7891000606ceeb87fa89689c045 content-encoding:gzip vary:Accept-Encoding server-timing:acl;dur=0.0, tok;dur=0.1, pack;dur=0.0, compress;dur=0.1;ratio=4.5, app;dur=12.0

>>> t.read()
16:58:27.134 -> GET 'https://tiled-demo.blueskyproject.io/table/partition/generated/short_table?partition=0' 'host:tiled-demo.blueskyproject.io' 'accept:application/vnd.apache.arrow.file' 'accept-encoding:gzip,blosc2' 'connection:keep-alive' 'user-agent:python-tiled/0.1.0a49.post0.dev0+g6dd1e5f' 'cookie:tiled_csrf=-fyaLez0YkradgcEVYBJh4QotR5MNyzouV0SV0NWHmM'
16:58:27.205 <- 200 server:nginx/1.18.0 (Ubuntu) date:Tue, 01 Feb 2022 21:58:27 GMT content-type:application/vnd.apache.arrow.file content-length:3847 connection:keep-alive etag:954688a8ef55915b012bba1e93769710 content-encoding:blosc2 vary:Accept-Encoding server-timing:acl;dur=0.0, read;dur=1.4, tok;dur=0.2, pack;dur=0.8, compress;dur=0.0;ratio=1.4, app;dur=9.1
              A         B         C
index
0      0.380618  0.761235  1.141853
1      0.259924  0.519848  0.779772
2      0.299569  0.599138  0.898707
3      0.469430  0.938859  1.408289
4      0.582898  1.165797  1.748695
...         ...       ...       ...
95     0.749854  1.499709  2.249563
96     0.279548  0.559096  0.838644
97     0.649457  1.298913  1.948370
98     0.481953  0.963907  1.445860
99     0.246197  0.492393  0.738590

[100 rows x 3 columns]
```

## Examine server performance with `server-timing`

The `server-timing` header is especially useful. While the tiled server is
handling our request, it records the time taken during each step of the
process. For example, at the end of the last line of the logs shown above, we
see:

```
server-timing:acl;dur=0.0, read;dur=1.4, tok;dur=0.2, pack;dur=0.8, compress;dur=0.0;ratio=1.4, app;dur=9.1
```

This follows a [standard syntax](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing).
The time units are **milliseconds**.

At the end of the line, `app;dur=` gives the total time, measured from when
the tiled server first received the client's request to the moment it began
transmitting its response. We can separate _application_ time from _network_
time by cross-referencing this number with the timestamps at the left of each
log line.

The item...

* `acl` gives time spent in authentication and access control enforcement;
* `read` gives time spent accessing the data;
* `tok` gives the time spent producing a fingerprint used for cache invalidation;
* `pack` gives the time spent encoding the data in the requested format;
* `compress` gives both the time spent compressing and the compression ratio
  achieved (higher is better).

## Turn off client logging

This undoes the logging configuration performed by `show_logs()`.
It does not affect any other logging configuration.

```py
from tiled.client import hide_logs

hide_logs()
```

## Collect requests and responses for advanced profiling

```python
from tiled.client import record_history

with record_history() as history:
    ...

history.requests  # list of Requests
history.responses  # list of Responses
```
