"""
This tests tiled's validation registry
"""

import numpy as np
import pandas as pd
import pytest
from starlette.status import HTTP_400_BAD_REQUEST

from ..client import Context, from_context
from ..config import merge
from ..server.app import build_app_from_config
from ..structures.core import StructureFamily
from ..validation_registration import ValidationError
from .utils import fail_with_status_code


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


def validate_foo(spec, metadata, entry, structure_family, structure):
    if structure_family != StructureFamily.table:
        raise ValidationError(f"structure family for spec {spec} must be 'table'")

    if list(structure.columns) != ["a", "b"]:
        raise ValidationError(f"structure for spec {spec} must have columns ['a', 'b']")

    metadata, metadata_modified = lower_case_dict(metadata)

    if "foo" not in metadata:
        raise ValidationError("metadata for spec {spec} must contain foo")

    if metadata_modified:
        return metadata


@pytest.fixture(scope="module")
def client(tmpdir_module):
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "path": "/",
                "args": {"writable_storage": str(tmpdir_module)},
            },
        ],
        "specs": [
            {"spec": "foo", "validator": f"{__name__}:validate_foo"},
            {"spec": "a"},
        ],
    }
    # Check that specs propagate correctly through merging configs.
    merged_config = merge({"filepath_placeholder": config})
    assert merged_config["specs"] is not None
    with Context.from_app(build_app_from_config(merged_config)) as context:
        yield from_context(context)


def test_validators(client):
    # valid example
    df = pd.DataFrame({"a": np.zeros(10), "b": np.zeros(10)})
    client.write_dataframe(df, metadata={"foo": 1}, specs=["foo"])

    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        # not expected structure family
        a = np.ones((5, 7))
        client.write_array(a, metadata={}, specs=["foo"])

    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        # column names are not expected
        df = pd.DataFrame({"x": np.zeros(10), "y": np.zeros(10)})
        client.write_dataframe(df, metadata={}, specs=["foo"])

    with fail_with_status_code(HTTP_400_BAD_REQUEST):
        # missing expected metadata
        df = pd.DataFrame({"a": np.zeros(10), "b": np.zeros(10)})
        client.write_dataframe(df, metadata={}, specs=["foo"])

    metadata = {"id": 1, "foo": "bar"}
    df = pd.DataFrame({"a": np.zeros(10), "b": np.zeros(10)})
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


def test_unknown_spec_strict(tmpdir):
    "Test unknown spec rejected for upload."
    config = {
        "trees": [
            {
                "tree": "tiled.catalog:in_memory",
                "path": "/",
                "args": {"writable_storage": str(tmpdir)},
            },
        ],
        "specs": [
            {"spec": "a"},
        ],
        "reject_undeclared_specs": True,
    }
    # Check that specs propagate correctly through merging configs.
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        a = np.ones((5, 7))
        client.write_array(a, metadata={}, specs=["a"])
        with fail_with_status_code(HTTP_400_BAD_REQUEST):
            # unknown spec 'b' should be rejected
            client.write_array(a, metadata={}, specs=["b"])


def test_unknown_spec_permissive(client):
    "Test unknown spec rejected for upload."
    a = np.ones((5, 7))
    client.write_array(a, metadata={}, specs=["a"])
    # unknown spec 'b' should be accepted
    client.write_array(a, metadata={}, specs=["b"])
