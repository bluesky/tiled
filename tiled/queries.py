"""
These objects describe queries, independent of how the actual querying is implemented.
They are used on the client side and the server side.

The are encoded into and decoded from URL query parameters.
"""

import enum
import json
from dataclasses import dataclass
from typing import Any, List, Optional

from .query_registration import register
from .structures.core import StructureFamily as StructureFamilyEnum

JSONSerializable = Any  # Feel free to refine this.


class NoBool:
    def __bool__(self):
        raise TypeError(
            """Queries are not "truth-y" or "false-y". They must be passed to search().

You may be seeing this error message because you tried to use a tiled query
with the Python keywords `and` or `or`. This does not (cannot) work.
To compose queries, chain search calls like:

    c.search(...).search(...).search(...)
"""
        )


@register(name="fulltext")
@dataclass
class FullText(NoBool):
    """
    Search the full text of all metadata values for word matches.

    This matches *complete words*, so 'dog' would match 'cat dog elephant',
    but 'do' would not match.

    Parameters
    ----------
    text : str
    """

    text: str

    def encode(self):
        return {"text": self.text}

    @classmethod
    def decode(cls, *, text):
        return cls(text=text)


@register(name="lookup")
@dataclass
class KeyLookup(NoBool):
    """
    Match a specific Entry by key. Mostly for internal use.

    This is necessary to support item lookup within search results, as in:

    >>> tree.search(...)["..."]

    The server handles this directly and generically, simply calling __getitem__
    on the tree after apply all other queries.  Implementations of search(...)
    do not need to handle it.

    Parameters
    ----------
    key : str
    """

    key: str

    def encode(self):
        return {"key": self.key}

    @classmethod
    def decode(cls, *, key):
        return cls(key=key)


@register(name="keys_filter")
@dataclass
class KeysFilter(NoBool):
    """
    Filter entries that do not match one of these keys.

    Parameters
    ----------
    keys : List[str]
    """

    keys: List[str]

    def encode(self):
        return {"keys": json.dumps(self.keys)}

    @classmethod
    def decode(cls, *, keys):
        return cls(keys=json.loads(keys))


@register(name="regex")
@dataclass
class Regex(NoBool):
    """
    Match a key's value to a regular expression.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    pattern : str
        regular expression
    case_sensitive : bool, optional
        Default True (case-sensitive).
        Note that this is the opposite of the default for FullText;
        regex users generally expect case sensitivity by default.

    Examples
    --------

    Search for color == "red"

    >>> c.search(Regex("sample.name", "Cu.*"))
    """

    key: str
    pattern: str
    case_sensitive: bool = True

    def encode(self):
        return {
            "key": self.key,
            "pattern": self.pattern,
            "case_sensitive": json.dumps(self.case_sensitive),
        }

    @classmethod
    def decode(cls, *, key, pattern, case_sensitive=True):
        # Note: FastAPI decodes case_sensitive into a boolean for us.
        return cls(
            key=key,
            pattern=pattern,
            case_sensitive=case_sensitive,
        )


@register(name="eq")
@dataclass
class Eq(NoBool):
    """
    Query equality of a given key's value to the specified value.

    See `Key` in this module for a more intuitive interface for equality.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for color == "red"

    >>> c.search(Eq("color", "red"))
    """

    key: str
    value: JSONSerializable

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


@register(name="noteq")
@dataclass
class NotEq(NoBool):
    """
    Query inequality of a given key's value to the specified value.

    See `Key` in this module for a more intuitive interface for inequality.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for color == "red"

    >>> c.search(Eq("color", "red"))
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
class Comparison(NoBool):
    """
    Query binary comparison between given key's value to the specified value.

    See `Key` in this module for a more intuitive interface for comparisons.

    Parameters
    ----------
    operator : {"gt", "lt", "ge", "le"}
    key : str
        e.g. "temperature"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for temperature > 300.

    >>> c.search(Comparison("gt", "temperature", 300))
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
class Contains(NoBool):
    """
    Query where a given key's value contains the specified value.

    Parameters
    ----------
    key : str
        e.g. "motors"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for matches where "ccd" is including the list of detectors.

    >>> c.search(Contains("detectors", "ccd"))
    """

    key: str
    value: JSONSerializable

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


@register(name="in")
@dataclass
class In:
    """
    Query if a given key's value is present in the specified sequence of values.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : List[JSONSerializable]
        e.g. ["red", "blue"]

    Examples
    --------

    Search for color in ["red", "blue"]

    >>> c.search(In("color", ["red", "blue"]))
    """

    key: str
    value: List[JSONSerializable]

    def __post_init__(self):
        self.value = list(self.value)

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


@register(name="notin")
@dataclass
class NotIn:
    """
    Query if a given key's value is not present in the specified sequence of values.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : List[JSONSerializable]
        e.g. ["red", "blue"]

    Examples
    --------

    Search for color not in ["red", "blue"]

    >>> c.search(NotIn("color", ["red", "blue"]))
    """

    key: str
    value: List[JSONSerializable]

    def __post_init__(self):
        self.value = list(self.value)

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


@register(name="keypresent")
@dataclass
class KeyPresent:
    """
    Query to retrieve containers that have a specific key at any level.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    exists : bool
        Set to True by default, but can be set to False to find the inverse
    Examples
    --------

    Search for containers that have the key "color"

    >>> c.search(KeyPresent("color"))

    Search for containers that do not have the key "sample.name"

    >>> c.search(KeyPresent("sample.name", exists=False))
    """

    key: str
    exists: bool = True

    def encode(self):
        return {"key": self.key, "exists": self.exists}

    @classmethod
    def decode(cls, *, key, exists):
        return cls(key=key, exists=exists)


@register(name="like")
@dataclass
class Like(NoBool):
    """
    Query for an approximate string match.

    This uses the SQL 'LIKE' operator.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    pattern : str
        e.g. "Ni%"

    Examples
    --------

    Search for sample starting with "Ni"

    >>> c.search(Like("sample", "Ni%"))

    Search for detector ending with "image"

    >>> c.search(Like("detector", "%image"))

    """

    key: str
    pattern: str

    def encode(self):
        return {"key": self.key, "pattern": json.dumps(self.pattern)}

    @classmethod
    def decode(cls, *, key, pattern):
        return cls(key=key, pattern=json.loads(pattern))


@register(name="specs")
@dataclass(init=False)
class SpecsQuery:
    """
    Query if specs list matches all elements in include list and does not match any element in exclude list

    Parameters
    ----------
    include : List[str]
    exclude : List[str]

    Examples
    --------

    Search for specs ["foo", "bar"] and NOT "baz"

    >>> c.search(SpecsQuery(include=["foo", "bar"], exclude=["baz"]))
    """

    include: List[str]
    exclude: List[str]

    def __init__(self, include, exclude=None):
        exclude = exclude or []

        if isinstance(include, str):
            raise TypeError("include must be a list not a str")

        if isinstance(exclude, str):
            raise TypeError("exclude must be a list not a str")

        self.include = list(include)
        self.exclude = list(exclude)

    def encode(self):
        return {
            "include": json.dumps(self.include),
            "exclude": json.dumps(self.exclude),
        }

    @classmethod
    def decode(cls, *, include, exclude):
        return cls(include=json.loads(include), exclude=json.loads(exclude))


def SpecQuery(spec):
    """
    Convenience function for querying if specs list contains a given spec

    Equivalent to SpecsQuery([spec]).

    Parameters
    ----------
    spec: str

    Examples
    --------

    Search for spec "foo"

    >>> c.search(Spec("foo"))
    """

    return SpecsQuery([spec])


@register(name="access_blob_filter")
@dataclass
class AccessBlobFilter:
    """
    Perform a query against the access_blob with two conditions.
    1. Query for a user id (i.e. username) match against the "user" field
    2. Query for if any tag in a list of tags is present in the "tags" field
    The values for these conditions are independent.

    Parameters
    ----------
    user_id : str
        e.g. "bill", "amanda"
    tags : List[JSONSerializable]
        e.g. ["tag_for_bill", "amanda_only"]


    Examples
    --------

    Search for user "bill", as well as tags in ["tag_for_bill", "useful_data"]

    >>> c.search(AccessBlobFilter("bill", ["tag_for_bill", "useful_data"]))
    """

    user_id: Optional[str]
    tags: List[str]

    def encode(self):
        return {
            "user_id": self.user_id,
            "tags": self.tags,
        }

    @classmethod
    def decode(cls, *, user_id, tags):
        return cls(
            user_id=user_id,
            tags=tags,
        )


@register(name="structure_family")
@dataclass(init=False)
class StructureFamilyQuery:
    """
    Query if structure_families match value

    Parameters
    ----------
    value : StructureFamily

    Examples
    --------

    Search for dataframes

    >>> c.search(StructureFamilies("dataframe"))
    """

    def __init__(self, value):
        self.value = StructureFamilyEnum(value)

    value: StructureFamilyEnum

    def encode(self):
        return {"value": self.value.value}

    @classmethod
    def decode(cls, *, value):
        return cls(value=value)


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

    Search for equality, comparison, or membership in a collection.

    >>> c.search(Key("color") == "red")
    >>> c.search(Key("temperature") >= 300)
    >>> c.search(Key("temperature") <= 300)
    >>> c.search(Key("position") > 5.0)
    >>> c.search(Key("position") < 5.0)
    """

    def __init__(self, key):
        self.key = key

    def __eq__(self, value):
        return Eq(self.key, value)

    def __ne__(self, value):
        return NotEq(self.key, value)

    def __lt__(self, value):
        return Comparison("lt", self.key, value)

    def __gt__(self, value):
        return Comparison("gt", self.key, value)

    def __le__(self, value):
        return Comparison("le", self.key, value)

    def __ge__(self, value):
        return Comparison("ge", self.key, value)

    # Note: __contains__ cannot be supported because the language coerces
    # the result of __contains__ to be a literal boolean. We are not
    # allowed to return a custom type.


class QueryValueError(ValueError):
    pass
