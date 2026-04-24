(webhooks)=

# Webhooks

```{warning}
Webhooks are an **experimental feature**. The API and configuration format may
change in future releases.
```

Webhooks let Tiled notify an external HTTP service whenever a catalog event
occurs — for example, when a new entry is created or a stream is closed.  This
is useful for triggering downstream pipelines, sending notifications, or
integrating Tiled with other systems without polling.

## Enabling webhooks

Webhooks are **disabled by default**.  Add a `webhooks:` section to your
server configuration file to turn them on:

```yaml
webhooks:
  secret_keys:
    - ${TILED_WEBHOOK_SECRET_KEY}
```

`secret_keys` is required.  Generate a key with:

```sh
openssl rand -hex 32
```

## Registering a webhook

Webhooks are registered against a node path.  Any event on that node — or any
of its descendants — will be delivered.

```sh
curl -X POST https://my-tiled-server/api/v1/webhooks/target/my/container \
  -H "Authorization: Apikey $TILED_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
        "url": "https://my-service.example.com/hooks/tiled",
        "secret": "my-hmac-secret",
        "events": ["container-child-created", "stream-closed"]
      }'
```

| Field    | Required | Description |
|----------|----------|-------------|
| `url`    | yes      | HTTPS endpoint that will receive `POST` requests |
| `secret` | no       | HMAC signing secret; requires `secret_keys` in server config |
| `events` | no       | List of event types to deliver; set `"events": null` or omit to receive all events |

Registering a webhook requires the `write:webhooks` scope (admin only).

The webhook URL **must use HTTPS**.

## Event types

| Type | When it fires |
|------|---------------|
| `container-child-created` | A new entry is created inside the watched container |
| `container-child-metadata-updated` | Metadata on a child entry is updated |
| `stream-closed` | A live-stream entry is closed |

Each delivery is a JSON `POST` with a body like:

```json
{
  "type": "container-child-created",
  "timestamp": "2025-01-15T12:34:56.789Z",
  "key": "scan_001",
  "path": ["my", "container"],
  "structure_family": "array",
  "specs": [],
  "metadata": {}
}
```

(verifying-deliveries)=
## Verifying deliveries

When a `secret` is set, Tiled adds an `X-Tiled-Signature` header to every
request:

```
X-Tiled-Signature: sha256=<hex-digest>
```

Verify it in your receiver:

```python
import hashlib, hmac

def verify(body: bytes, secret: str, header: str) -> bool:
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, header)
```

Each delivery also carries an `X-Tiled-Event-ID` header — a unique hex string
you can use to deduplicate retried deliveries, for example:

```python
seen = set()
def handle(request):
    event_id = request.headers["X-Tiled-Event-ID"]
    if event_id in seen:
        return  # duplicate delivery, ignore
    seen.add(event_id)
    ...
```

## Managing webhooks

**List** webhooks registered on a node:

```sh
curl https://my-tiled-server/api/v1/webhooks/target/my/container \
  -H "Authorization: Apikey $TILED_API_KEY"
```

**Delete** a webhook (use the `id` returned when you registered it):

```sh
curl -X DELETE https://my-tiled-server/api/v1/webhooks/42 \
  -H "Authorization: Apikey $TILED_API_KEY"
```

**View delivery history** for a webhook:

```sh
curl "https://my-tiled-server/api/v1/webhooks/history/42?limit=20" \
  -H "Authorization: Apikey $TILED_API_KEY"
```

History records are retained for 30 days and then pruned automatically.

## Retry behaviour

If the target URL returns a non-2xx response or is unreachable, Tiled retries
up to **3 times** with exponential back-off (roughly 1 s → 5 s → 25 s, plus
random jitter).  The final outcome (`success` or `failed`) is recorded in the
delivery history.

## Security notes

* Webhook URLs must use HTTPS.
* Registering and listing webhooks requires the `write:webhooks` / `read:webhooks`
  scope, which is granted to admin users only.  This is the primary access-control
  layer preventing unauthorized webhook registration.
* At registration time, Tiled additionally checks that the target URL does not
  resolve to a private, loopback, or link-local address (SSRF protection).
  Note that this hostname-level check can be bypassed by DNS-rebinding attacks;
  for high-security deployments also route outbound webhook requests through a
  network-level egress proxy (e.g.
  [Smokescreen](https://github.com/stripe/smokescreen)).
* HMAC signing secrets are encrypted at rest using the server's `secret_keys`.
