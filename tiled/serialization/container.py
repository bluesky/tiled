import io

from ..media_type_registration import default_serialization_registry
from ..structures.core import StructureFamily
from ..utils import (
    BrokenLink,
    SerializationError,
    ensure_awaitable,
    modules_available,
    safe_json_dump,
)


async def walk(node, filter_for_access, pre=None):
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
    if node.structure_family != StructureFamily.array:
        filtered = await filter_for_access(node)
        if hasattr(filtered, "items_range"):
            for key, value in await filtered.items_range(0, None):
                async for d in walk(value, filter_for_access, pre + [key]):
                    yield d
        elif node.structure_family == StructureFamily.table:
            for key in node.structure().columns:
                yield (pre + [key], filtered)
        else:
            for key, value in filtered.items():
                async for d in walk(value, filter_for_access, pre + [key]):
                    yield d
    else:
        yield (pre, node)


if modules_available("h5py"):

    async def serialize_hdf5(mimetype, node, metadata, filter_for_access):
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
            async for key_path, array_adapter in walk(node, filter_for_access):
                group = file
                node = root_node
                for key in key_path[:-1]:
                    if hasattr(node, "lookup_adapter"):
                        try:
                            node = await node.lookup_adapter([key])
                        except BrokenLink:
                            continue
                    else:
                        node = node[key]
                    if key in group:
                        group = group[key]
                    else:
                        group = group.create_group(key)
                        try:
                            group.attrs.update(node.metadata())
                        except TypeError:
                            raise SerializationError(MSG)
                data = await ensure_awaitable(array_adapter.read)
                dataset = group.create_dataset(key_path[-1], data=data)
                for k, v in array_adapter.metadata().items():
                    dataset.attrs.create(k, v)
        return buffer.getbuffer()

    default_serialization_registry.register(
        StructureFamily.container, "application/x-hdf5", serialize_hdf5
    )

if modules_available("orjson"):

    async def serialize_json(mimetype, node, metadata, filter_for_access):
        "Export node to JSON, with each node having a 'contents' and 'metadata' sub-key."
        root_node = node
        to_serialize = {"contents": {}, "metadata": dict(root_node.metadata())}
        async for key_path, array_adapter in walk(node, filter_for_access):
            d = to_serialize["contents"]
            node = root_node
            for key in key_path:
                if hasattr(node, "lookup_adapter"):
                    try:
                        node = await node.lookup_adapter([key])
                    except BrokenLink:
                        continue
                else:
                    node = node[key]
                if key not in d:
                    d[key] = {"contents": {}, "metadata": dict(node.metadata())}
                d = d[key]["contents"]
        return safe_json_dump(to_serialize)

    default_serialization_registry.register(
        StructureFamily.container, "application/json", serialize_json
    )
