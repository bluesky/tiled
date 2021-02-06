"""
These objects express high-level queries and translate them (when possible)
into concrete queries for specific storage backends.
"""
# This is roughly a "poor man's" implementation of what apischema does.
# It could be revamped to use apischema in the future.

from dataclasses import dataclass, asdict
import json


_Query_subclasses = {}


class Query:
    def __init_subclass__(cls, **kwargs):
        _Query_subclasses[cls.__name__] = cls


def serialize(query):
    return json.dumps({type(query).__name__: asdict(query)}).encode()


def deserialize(bytes_):
    dict_ = json.loads(bytes_.decode())
    name = next(iter(dict_))
    cls = _Query_subclasses[name]
    return cls(**dict_[name])


@dataclass
class Text(Query):
    text: str
