# Control Retries

Tiled uses the library [stamina](https://stamina.hynek.me/en/stable/) to
implement HTTP retries. Retries are essential for making distributed systems
resilient. See the
[Motivation](https://stamina.hynek.me/en/stable/motivation.html) section of the
stamina documentation for more.

See following examples to control retry behavior.

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
