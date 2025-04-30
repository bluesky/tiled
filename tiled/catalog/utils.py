import hashlib

import canonicaljson


def compute_structure_id(structure):
    "Compute HEX digest of MD5 hash of RFC 8785 canonical form of JSON."
    canonical_structure = canonicaljson.encode_canonical_json(structure)

    return hashlib.md5(canonical_structure).hexdigest()
