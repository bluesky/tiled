from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.core import Spec


def test_specs():
    tree = MapAdapter({}, specs=[Spec("spec_test")])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test")]


def test_specs_give_as_str():
    tree = MapAdapter({}, specs=["spec_test"])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test")]


def test_specs_with_version():
    tree = MapAdapter({}, specs=[Spec("spec_test", version="1.1")])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test", version="1.1")]
