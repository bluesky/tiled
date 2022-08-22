"""
This tests tiled's validation registry
"""

import numpy as np
import pandas as pd

from ..client import from_tree
from ..validation_registration import ValidationError, ValidationRegistry
from .utils import fail_with_status_code
from .writable_adapters import WritableMapAdapter

API_KEY = "secret"


def lower_case_dict(d):
    out = {}
    modified = False

    for k, v in d.items():
        if isinstance(k, str) and not k.islower():
            out[k.lower()] = v
            modified = True
        else:
            out[k] = v

    return out, modified


def validate_foo(metadata, structure_family, structure, spec):
    if structure_family != "dataframe":
        raise ValidationError(f"structure family for spec {spec} must be dataframe")

    if list(structure.macro.columns) != ["a", "b"]:
        raise ValidationError(f"structure for spec {spec} must have columns ['a', 'b']")

    metadata, metadata_modified = lower_case_dict(metadata)

    if "foo" not in metadata:
        raise ValidationError("metadata for spec {spec} must contain foo")

    if metadata_modified:
        return metadata


def test_validators():

    validation_registry = ValidationRegistry()
    validation_registry.register("foo", validate_foo)

    tree = WritableMapAdapter({})
    client = from_tree(
        tree,
        api_key=API_KEY,
        authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    with fail_with_status_code(400):
        a = np.ones((5, 7))
        client.write_array(a, metadata={}, specs=["foo"])

    with fail_with_status_code(400):
        df = pd.DataFrame({"x": np.zeros(100), "y": np.zeros(100)})
        client.write_dataframe(df, metadata={}, specs=["foo"])

    with fail_with_status_code(400):
        df = pd.DataFrame({"a": np.zeros(100), "b": np.zeros(100)})
        client.write_dataframe(df, metadata={}, specs=["foo"])

    metadata = {"id": 1, "foo": "bar"}
    df = pd.DataFrame({"a": np.zeros(100), "b": np.zeros(100)})
    result = client.write_dataframe(df, metadata=metadata, specs=["foo"])
    assert result.metadata == metadata
    result_df = result.read()
    pd.testing.assert_frame_equal(result_df, df)

    metadata_upper = {"ID": 2, "FOO": "bar"}
    metadata_lower, _ = lower_case_dict(metadata_upper)
    result = client.write_dataframe(df, metadata=metadata_upper, specs=["foo"])
    assert result.metadata == metadata_lower
    result_df = result.read()
    pd.testing.assert_frame_equal(result_df, df)
