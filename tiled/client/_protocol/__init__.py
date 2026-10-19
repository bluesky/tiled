"""Pure, I/O-free protocol knowledge shared by the Tiled client shells.

The modules in this package hold the client's protocol knowledge -- request
builders, response parsers, and failure classifiers -- as pure functions and
dataclasses with no I/O. The client classes ("shells") turn `RequestSpec`s
into actual HTTP requests, own retries/throttling/progress reporting, and
interpret responses with the matching parser functions. Keeping this layer
pure makes it directly unit-testable and shareable between synchronous and
asynchronous client implementations.
"""
