import uuid

from ..adapters.mapping import MapAdapter
from ..client import from_tree

values = ["a", "b", "c"]
counts = [10, 5, 7]

mapping = {}

for value, count in zip(values, counts):
    for _ in range(count):
        mapping[str(uuid.uuid4())] = MapAdapter({}, metadata={"foo": {"bar": value}})

# items which do not contain the queries metadata should not effect the results
for _ in range(10):
    mapping[str(uuid.uuid4())] = MapAdapter({}, metadata={})

tree = MapAdapter(mapping)


def test_distinct():
    client = from_tree(tree)

    distinct = client.distinct(metadata_keys=["foo.bar"], counts=False)
    expected = {"foo.bar": [{"value": v, "count": None} for v in values]}
    assert distinct["metadata"] == expected

    distinct = client.distinct(metadata_keys=["foo.bar"], counts=True)
    expected = {"foo.bar": [{"value": v, "count": c} for v, c in zip(values, counts)]}
    assert distinct["metadata"] == expected

    distinct = client.distinct(metadata_keys=["baz"], counts=True)
    expected = {"baz": []}
    assert distinct["metadata"] == expected
