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


def expand_term(term: str, namespaces: dict[str, str]) -> str:
    parsed = split_curie(term)
    if parsed is None:
        return term
    prefix, local = parsed
    namespace = namespaces.get(prefix)
    if namespace is None:
        return term
    return f"{namespace}{local}"


def compact_term(term: str, namespaces: dict[str, str]) -> str:
    for prefix, namespace in sorted(
        namespaces.items(), key=lambda item: len(item[1]), reverse=True
    ):
        if term.startswith(namespace):
            start = len(namespace)
            local = term[start:]
            if local:
                return f"{prefix}:{local}"
    return term


def expand_value(value, namespaces: dict[str, str]):
    if isinstance(value, dict):
        return {
            expand_term(key, namespaces): expand_value(inner, namespaces)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [expand_value(item, namespaces) for item in value]
    return value


def compact_value(value, namespaces: dict[str, str]):
    if isinstance(value, dict):
        return {
            compact_term(key, namespaces): compact_value(inner, namespaces)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [compact_value(item, namespaces) for item in value]
    return value


def collect_used_prefixes_from_term(term: str, namespaces: dict[str, str]) -> set[str]:
    used: set[str] = set()

    parsed = split_curie(term)
    if parsed is not None:
        prefix, _ = parsed
        if prefix in namespaces:
            used.add(prefix)

    for prefix, namespace in namespaces.items():
        if term.startswith(namespace):
            used.add(prefix)
    return used


def collect_used_prefixes_from_value(value, namespaces: dict[str, str]) -> set[str]:
    used: set[str] = set()
    if isinstance(value, dict):
        for key, inner in value.items():
            if isinstance(key, str):
                used.update(collect_used_prefixes_from_term(key, namespaces))
            used.update(collect_used_prefixes_from_value(inner, namespaces))
    elif isinstance(value, list):
        for item in value:
            used.update(collect_used_prefixes_from_value(item, namespaces))
    return used
