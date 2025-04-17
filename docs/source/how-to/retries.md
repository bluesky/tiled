# Control Retries

Tiled uses the library [stamina](https://stamina.hynek.me/en/stable/) to
implement HTTP retries. Retries are essential for making distributed systems
resilient. See the
[Motivation](https://stamina.hynek.me/en/stable/motivation.html) section of the
stamina documentation for more.

Tiled retries requests that fail to garner a response (e.g. due to connection
problems) or receive a response indicating a server-side problem (HTTP
status code `5XX`). By default, it retries for 10 attempts or 45 seconds,
whichever it reaches first. These defaults can be tuned by setting
environment variables:

```
TILED_RETRY_ATTEMPTS  # max number of attempts
TILED_RETRY_TIMEOUT  # max total seconds
```

See following examples to control retry behavior in the context of development
or testing.

```python
import stamina

# Disable retries globally.
stamina.set_active(False)

# Check whether retries are active.
stamina.is_active()

# Disable delay between attempts
stamina.set_testing(True, attempts=1)

# Check whether test mode is enabled.
stamina.is_testing()
```
