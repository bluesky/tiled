import io

from ..media_type_registration import serialization_registry
from ..utils import SerializationError, modules_available, safe_json_dump


def walk(node, pre=None):
    """
    Yield (key_path, value) where each value is an ArrayAdapter.

    As a succinct illustration (does not literally run):

    >>> list(walk{"a": {"b": 1, "c": {"d": 2}}})
    [
        (("a", "b"), 1),
        (("a", "c", "d"), 2),
    ]
    """
    pre = pre[:] if pre else []
    if node.structure_family != "array":
        for key, value in node.items():
            for d in walk(value, pre + [key]):
                yield d
    else:
        yield (pre, node)


if modules_available("h5py"):

    def serialize_hdf5(node, metadata):
        """
        Encode everything below this node as HDF5.

        Walk node. Encode all nodes an dataframes as Groups, all arrays and columns as Datasets.
        """
        import h5py

        buffer = io.BytesIO()
        root_node = node
        MSG = "Metadata contains types or structure that does not fit into HDF5."
        with h5py.File(buffer, mode="w") as file:
            try:
                file.attrs.update(metadata)
            except TypeError:
                raise SerializationError(MSG)
            for key_path, array_adapter in walk(node):
                group = file
                node = root_node
                for key in key_path[:-1]:
                    node = node[key]
                    if key in group:
                        group = group[key]
                    else:
                        group = group.create_group(key)
                        try:
                            group.attrs.update(node.metadata)
                        except TypeError:
                            raise SerializationError(MSG)
                data = array_adapter.read()
                dataset = group.create_dataset(key_path[-1], data=data)
                for k, v in array_adapter.metadata.items():
                    dataset.attrs.create(k, v)
        return buffer.getbuffer()

    serialization_registry.register("node", "application/x-hdf5", serialize_hdf5)

if modules_available("orjson"):

    def serialize_json(node, metadata):
        "Export node to JSON, with each node having a 'contents' and 'metadata' sub-key."
        root_node = node
        to_serialize = {"contents": {}, "metadata": dict(root_node.metadata)}
        for key_path, array_adapter in walk(node):
            d = to_serialize["contents"]
            node = root_node
            for key in key_path:
                node = node[key]
                if key not in d:
                    d[key] = {"contents": {}, "metadata": dict(node.metadata)}
                d = d[key]["contents"]
        return safe_json_dump(to_serialize)

    serialization_registry.register("node", "application/json", serialize_json)
