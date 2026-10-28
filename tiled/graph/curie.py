"""
CURIE (Compact URI) expansion/compaction against the namespace registry.

Used by the GraphQL schema to expand terms written through mutations and
compact terms read back out, so prefixes resolve consistently.
"""

from __future__ import annotations

from typing import Optional


def split_curie(term: str) -> Optional[tuple[str, str]]:
    if term.startswith("@"):
        return None
    if ":" not in term or "://" in term:
        return None
    prefix, local = term.split(":", 1)
    if not prefix or not local:
        return None
    return prefix, local


def expand_term(term: str, namespaces) -> str:
    parsed = split_curie(term)
    if parsed is None:
        return term
    prefix, local = parsed
    namespace = namespaces.get(prefix)
    if namespace is None:
        return term
    return f"{namespace}{local}"


class Namespaces:
    """
    A prefix -> URI registry with the compaction order computed once.

    Compacting a term tries the longest namespace URI first, so the registry
    must be sorted by URI length. Sorting on every `compact_term` call is
    wasteful when a single request compacts hundreds of keys and predicates
    against the same registry; this object sorts once and is reused for the
    whole request. It behaves like the plain `dict` it wraps for lookups.
    """

    __slots__ = ("mapping", "by_length")

    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping
        self.by_length = sorted(
            mapping.items(), key=lambda item: len(item[1]), reverse=True
        )

    def get(self, prefix: str, default=None):
        return self.mapping.get(prefix, default)

    def items(self):
        return self.mapping.items()

    def __iter__(self):
        return iter(self.mapping)

    def __len__(self):
        return len(self.mapping)

    def __contains__(self, prefix) -> bool:
        return prefix in self.mapping


def _by_length(namespaces) -> list[tuple[str, str]]:
    if isinstance(namespaces, Namespaces):
        return namespaces.by_length
    return sorted(namespaces.items(), key=lambda item: len(item[1]), reverse=True)


def compact_term(term: str, namespaces) -> str:
    for prefix, namespace in _by_length(namespaces):
        if term.startswith(namespace):
            start = len(namespace)
            local = term[start:]
            if local:
                return f"{prefix}:{local}"
    return term


def expand_value(value, namespaces):
    if isinstance(value, dict):
        return {
            expand_term(key, namespaces): expand_value(inner, namespaces)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [expand_value(item, namespaces) for item in value]
    return value


def compact_value(value, namespaces):
    if isinstance(value, dict):
        return {
            compact_term(key, namespaces): compact_value(inner, namespaces)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [compact_value(item, namespaces) for item in value]
    return value
