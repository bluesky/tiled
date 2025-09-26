import pytest
from pydantic import TypeAdapter, ValidationError

from ..type_aliases import EntryPointString

type_adapter = TypeAdapter(EntryPointString)


@pytest.mark.parametrize(
    ("s", "args"),
    [
        # callables and valid arguments for them
        ("json:dumps", ({"a": 1},)),  # function in package
        ("os.path:join", ("a", "b")),  # function in submodule
        ("datetime:datetime.now", ()),  # method
    ],
)
def test_valid_input(s, args):
    result = type_adapter.validate_python(s)
    # Verify that we have the real method by calling it.
    result(*args)


@pytest.mark.parametrize(
    "s",
    [
        "",  # empty
        ":",  # empty parts
        "nonexistent:module",  # bad module
        "os:nonexistent",  # bad attribute
        "os.path:nonexistent.method",  # bad nested attribute
        "os.path.join",  # dotted syntax (not allowed)
        "datetime.datetime.now",  # dotted syntax (not allowed)
    ],
)
def test_invalid_input(s):
    with pytest.raises(ValidationError):
        type_adapter.validate_python(s)
