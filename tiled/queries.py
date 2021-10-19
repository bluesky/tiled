from dataclasses import dataclass

from .query_registration import register


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


class QueryValueError(ValueError):
    pass
