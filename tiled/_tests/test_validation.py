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


def validate_foo(metadata, structure_family, structure, spec):
    if structure_family != "dataframe":
        raise ValidationError(f"structure family for spec {spec} must be dataframe")

    if list(structure.macro.columns) != ["a", "b"]:
        raise ValidationError(f"structure for spec {spec} must have columns ['a', 'b']")

    if "foo" not in metadata:
        raise ValidationError("metadata for spec {spec} must contain foo")


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

    df = pd.DataFrame({"a": np.zeros(100), "b": np.zeros(100)})
    client.write_dataframe(df, metadata={"foo": "bar"}, specs=["foo"])

    assert len(client.values()) == 1

    result = client.values().first()

    result_df = result.read()
    pd.testing.assert_frame_equal(result_df, df)
