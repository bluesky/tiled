---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.17.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Webhooks

```{warning}
Webhooks are an **experimental feature**. The API and configuration format may
change in future releases.
```

While Tiled's {ref}`streaming subscriptions <stream>` push
data to a Python client over a WebSocket, **webhooks** push to any external
HTTP service — no persistent connection required.  Whenever a catalog event
fires (new entry created, metadata updated, stream closed), Tiled sends an
HTTP `POST` containing a JSON description of the event to a URL you register.

This makes webhooks a good fit for:

- triggering downstream analysis pipelines
- sending notifications (Slack, email, etc.)
- integrating Tiled with systems that cannot maintain a long-lived connection

This tutorial demonstrates the full webhook lifecycle end-to-end in a single
Python session, with no external services and no configuration files required.

## Set up a local receiver

In production your webhook target would be an existing web service.  Here we
spin up a tiny stdlib HTTP server on a background thread to capture the
incoming `POST` requests.

```{code-cell} ipython3
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

received = []  # payloads land here


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        received.append(json.loads(self.rfile.read(length)))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass  # silence per-request logs


receiver = HTTPServer(("127.0.0.1", 0), _Handler)
receiver_port = receiver.server_address[1]
threading.Thread(target=receiver.serve_forever, daemon=True).start()

print(f"Receiver listening on http://127.0.0.1:{receiver_port}/hook")
```

## Start a Tiled server with webhooks enabled

`SimpleTiledServer` accepts `enable_webhooks=True`, which automatically
generates a signing-secret key and relaxes the URL validator so that plain
`http://localhost` targets are accepted — convenient for local development and
tutorials.

```{code-cell} ipython3
from tiled.server import SimpleTiledServer
from tiled.client import from_uri

server = SimpleTiledServer(enable_webhooks=True)
client = from_uri(server.uri)

print(f"Tiled server running at {server.uri}")
```

## Register a webhook

A webhook is registered against a **node path**.  Any event on that node, or
any of its descendants, will be delivered.

Registering on the root (`""`) means we watch the entire catalog.  Omitting
`"events"` means all event types are delivered.

```{code-cell} ipython3
import json
import httpx

resp = httpx.post(
    f"http://localhost:{server.port}/api/v1/webhooks/target/",
    headers={
        "Authorization": f"Apikey {server.api_key}",
        "Content-Type": "application/json",
    },
    content=json.dumps({"url": f"http://127.0.0.1:{receiver_port}/hook"}),
)
resp.raise_for_status()

webhook = resp.json()
webhook_id = webhook["id"]
print(f"Webhook registered (id={webhook_id})")
```

## Write data and watch the deliveries arrive

Every `write_array` call creates a new catalog entry, which triggers a
`container-child-created` event.  Tiled dispatches the delivery in the
background, so we wait briefly before inspecting what the receiver collected.

```{code-cell} ipython3
import time
import numpy as np

client.write_array(np.array([1.0, 2.0, 3.0]), key="temperature")
client.write_array(np.array([10, 20, 30]), key="counts")

# Wait for background deliveries
deadline = time.monotonic() + 10
while len(received) < 2 and time.monotonic() < deadline:
    time.sleep(0.1)

print(f"Received {len(received)} delivery/deliveries")
```

Each delivery is a plain JSON object describing the event:

```{code-cell} ipython3
for payload in received:
    print(json.dumps(payload, indent=2))
    print()
```

The key fields are:

| Field | Description |
|-------|-------------|
| `type` | The event type, e.g. `container-child-created` |
| `key` | The name of the new or updated entry |
| `path` | Full path from the catalog root |
| `structure_family` | `array`, `table`, `container`, … |
| `specs` | Any specs attached to the entry |
| `metadata` | The entry's metadata at the time of the event |

## Verify with HMAC signatures

When you register a webhook with a `"secret"`, Tiled adds an
`X-Tiled-Signature` header to every request so your receiver can confirm
the payload was not tampered with.

```python
import hashlib, hmac

def verify(body: bytes, secret: str, header: str) -> bool:
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, header)
```

Each delivery also carries an `X-Tiled-Event-ID` header — a unique hex string
useful for deduplicating retried deliveries:

```python
seen = set()

def handle(request):
    event_id = request.headers["X-Tiled-Event-ID"]
    if event_id in seen:
        return  # duplicate, ignore
    seen.add(event_id)
    process(request.body)
```

## Inspect delivery history

Tiled records every delivery attempt.  This is useful for debugging: you can
see whether a delivery succeeded, how many retries it took, and what HTTP
status code your receiver returned.

```{code-cell} ipython3
resp = httpx.get(
    f"http://localhost:{server.port}/api/v1/webhooks/history/{webhook_id}",
    headers={"Authorization": f"Apikey {server.api_key}"},
)
history = resp.json()

print(f"Delivery history ({len(history)} record(s)):\n")
for record in history:
    print(f"  event_type : {record['event_type']}")
    print(f"  outcome    : {record['outcome']}")
    print(f"  attempts   : {record['attempts']}")
    print(f"  status     : {record['status_code']}")
    print()
```

If the target URL returns a non-2xx response or is unreachable, Tiled retries
up to **3 times** with exponential back-off (roughly 1 s → 5 s → 25 s, plus
random jitter).

## Clean up

```{code-cell} ipython3
receiver.shutdown()
server.close()
```

## See also

- {doc}`../user-guide/webhooks` — operator reference: server configuration,
  HMAC signing, SSRF protection, managing webhooks via the API
- {doc}`10-minutes-to-tiled` — broader tour of Tiled's write, stream, and
  register capabilities
