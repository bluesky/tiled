import operator
import warnings

_MESSAGE = (
    "Instead of {name}_indexer[...] use {name}()[...]. "
    "The {name}_indexer accessor is deprecated."
)


class IndexersMixin:
    """
    Provides slicable attributes keys_indexer, items_indexer, values_indexer.

    This is just for back-ward compatiblity.
    """

    @property
    def keys_indexer(self):
        warnings.warn(_MESSAGE.format(name="keys"), DeprecationWarning)
        return self.keys()

    @property
    def values_indexer(self):
        warnings.warn(_MESSAGE.format(name="values"), DeprecationWarning)
        return self.values()

    @property
    def items_indexer(self):
        warnings.warn(_MESSAGE.format(name="items"), DeprecationWarning)
        return self.items()


def tree_repr(tree, sample):
    sample_reprs = list(map(repr, sample))
    out = f"<{type(tree).__name__} {{"
    # Always show at least one.
    if sample_reprs:
        out += sample_reprs[0]
    # And then show as many more as we can fit on one line.
    counter = 1
    for sample_repr in sample_reprs[1:]:
        if len(out) + len(sample_repr) > 60:  # character count
            break
        out += ", " + sample_repr
        counter += 1
    approx_len = operator.length_hint(tree)  # cheaper to compute than len(tree)
    # Are there more in the tree that what we displayed above?
    if approx_len > counter:
        out += f", ...}} ~{approx_len} entries>"
    else:
        out += "}>"
    return out


class IndexCallable:
    """
    DEPRECATED and no longer used internally

    Provide getitem syntax for functions

    >>> def inc(x):
    ...     return x + 1

    >>> I = IndexCallable(inc)
    >>> I[3]
    4

    Vendored from dask
    """

    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn

    def __getitem__(self, key):
        return self.fn(key)
