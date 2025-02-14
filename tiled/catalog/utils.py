import hashlib

import canonicaljson

from ..utils import ensure_uri


def compute_structure_id(structure):
    "Compute HEX digest of MD5 hash of RFC 8785 canonical form of JSON."
    canonical_structure = canonicaljson.encode_canonical_json(structure)

    return hashlib.md5(canonical_structure).hexdigest()


def classify_writable_storage(uris: list[str]) -> dict[str, str]:
    result = {}
    for item in uris:
        item_uri = ensure_uri(item)
        if item_uri.startswith("file:"):
            if "filesystem" in result:
                raise NotImplementedError("Can only write to one filesystem location")
            result["filesystem"] = item_uri
        elif (
            item_uri.startswith("duckdb:")
            or item_uri.startswith("sqlite:")
            or item_uri.startswith("postgresql:")
        ):
            if "sql" in result:
                raise NotImplementedError("Can only write to one SQL database")
            result["sql"] = item_uri
        else:
            raise ValueError(
                "Unrecognized writable location {item}. "
                "Input should be a filepath or URI beginning with "
                "'file:', 'duckdb:', or 'postgresql:'."
            )
    return result
