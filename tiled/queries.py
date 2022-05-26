"""
These objects describe queries, independent of how the actual querying is implemented.
They are used on the client side and the server side.

The are encoded into and decoded from URL query parameters.
"""

import enum
import json
from dataclasses import dataclass
from typing import Any

from .query_registration import register

JSONSerializable = Any  # Feel free to refine this.


@register(name="fulltext")
@dataclass
class FullText:
    """
    Search the full text of all metadata values for word matches.

    This matches *complete words*, so 'dog' would match 'cat dog elephant',
    but 'do' would not match.
    """

    text: str
    case_sensitive: bool = False

    def encode(self):
        return {"text": self.text, "case_sensitive": json.dumps(self.case_sensitive)}

    @classmethod
    def decode(cls, *, text, case_sensitive="false"):
        return cls(
            text=text,
            case_sensitive=json.loads(case_sensitive),
        )


@register(name="lookup")
@dataclass
class KeyLookup:
    """
    Match a specific Entry by key. Mostly for internal use.

    This is necessary to support item lookup within search results, as in:

    >>> tree.search(...)["..."]

    The server handles this directly and generically, simply calling __getitem__
    on the tree after apply all other queries.  Implementations of search(...)
    do not need to handle it.
    """

    key: str

    def encode(self):
        return {"key": self.key}

    @classmethod
    def decode(cls, *, key):
        return cls(key=key)


@register(name="eq")
@dataclass
class Eq:
    """
    Query equality of a given key's value to the specified value.

    """

    key: str
    value: JSONSerializable

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


class Operator(str, enum.Enum):
    lt = "lt"
    gt = "gt"
    le = "le"
    ge = "ge"


@register(name="comparison")
@dataclass
class Comparison:
    """
    Query binary comparison between given key's value to the specified value.

    """

    operator: Operator
    key: str
    value: JSONSerializable

    def __init__(self, operator, key, value):
        self.operator = Operator(operator)
        self.key = key
        self.value = value

    def encode(self):
        return {
            "operator": self.operator.value,
            "key": self.key,
            "value": json.dumps(self.value),
        }

    @classmethod
    def decode(cls, *, operator, key, value):
        return cls(operator=Operator(operator), key=key, value=json.loads(value))


@register(name="contains")
@dataclass
class Contains:
    """
    Query where a given key's value contains the specified value.

    """

    key: str
    value: JSONSerializable

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


class Key:
    """
    Compare a key in the metadata to a value using standard Python operators.

    This itself is not a query, but *comparing* it with a value, as shown
    in the examples below, produces a query.

    Parameters
    ----------
    key : str

    Examples
    --------

    >>> c.search(Key("color") == "red")
    >>> c.search(Key("temperature") >= 300)
    >>> c.search(Key("temperature") <= 300)
    >>> c.search(Key("position") > 5.0)
    >>> c.search(Key("position") < 5.0)
    >>> c.search("current" in Key("detectors"))
    """

    def __init__(self, key):
        self.key = key

    def __eq__(self, value):
        return Eq(self.key, value)

    def __lt__(self, value):
        return Comparison("lt", self.key, value)

    def __gt__(self, value):
        return Comparison("gt", self.key, value)

    def __le__(self, value):
        return Comparison("le", self.key, value)

    def __ge__(self, value):
        return Comparison("ge", self.key, value)


class QueryValueError(ValueError):
    pass
