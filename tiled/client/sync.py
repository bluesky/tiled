import itertools

from ..structures.core import StructureFamily
from ..structures.data_source import DataSource, Management
from .base import BaseClient


def copy(
    source: BaseClient,
    dest: BaseClient,
):
    """
    Copy data from one Tiled instance to another.

    Parameters
    ----------
    source : tiled node
    dest : tiled node

    Examples
    --------

    Connect to two instances and copy data.

    >>> from tiled.client import from_uri
    >>> from tiled.client.sync import copy
    >>> a = from_uri("http://localhost:8000", api_key="secret")
    >>> b = from_uri("http://localhost:9000", api_key="secret")
    >>> copy(a, b)


    Copy select data.

    >>> copy(a.items().head(), b)
    >>> copy(a.search(...), b)

    """
    if hasattr(source, "structure_family"):
        # looks like a client object
        _DISPATCH[source.structure_family](source.include_data_sources(), dest)
    else:
        _DISPATCH[StructureFamily.container](dict(source), dest)


def _copy_array(source, dest):
    num_blocks = (range(len(n)) for n in source.chunks)
    # Loop over each block index --- e.g. (0, 0), (0, 1), (0, 2) ....
    for block in itertools.product(*num_blocks):
        array = source.read_block(block)
        dest.write_block(array, block)


def _copy_awkward(source, dest):
    import awkward

    array = source.read()
    _form, _length, container = awkward.to_buffers(array)
    dest.write(container)


def _copy_sparse(source, dest):
    num_blocks = (range(len(n)) for n in source.chunks)
    # Loop over each block index --- e.g. (0, 0), (0, 1), (0, 2) ....
    for block in itertools.product(*num_blocks):
        array = source.read_block(block)
        dest.write_block(array.coords, array.data, block)


def _copy_table(source, dest):
    for partition in range(source.structure().npartitions):
        df = source.read_partition(partition)
        dest.write_partition(df, partition)


def _copy_container(source, dest):
    for key, child_node in source.items():
        original_data_sources = child_node.include_data_sources().data_sources()
        num_data_sources = len(original_data_sources)
        if num_data_sources == 0:
            # A container with no data sources is just an organizational
            # entity in the database.
            if child_node.structure_family == StructureFamily.container:
                data_sources = []
            else:
                raise ValueError(
                    f"Unable to copy {child_node} which is a "
                    f"{child_node.structure_family} but has no data sources."
                )
        elif num_data_sources == 1:
            (original_data_source,) = original_data_sources
            if original_data_source.management == Management.external:
                data_sources = [original_data_source]
            else:
                if child_node.structure_family == StructureFamily.container:
                    data_sources = []
                else:
                    data_sources = [
                        DataSource(
                            management=original_data_source.management,
                            mimetype=original_data_source.mimetype,
                            structure_family=original_data_source.structure_family,
                            structure=original_data_source.structure,
                        )
                    ]
        else:
            # As of this writing this is impossible, but we anticipate that
            # it may be added someday.
            raise NotImplementedError(
                "Multiple Data Sources in one Node is not supported."
            )
        node = dest.new(
            key=key,
            structure_family=child_node.structure_family,
            data_sources=data_sources,
            metadata=dict(child_node.metadata),
            specs=child_node.specs,
        )
        if (
            original_data_sources
            and (original_data_sources[0].management != Management.external)
        ) or (
            child_node.structure_family == StructureFamily.container
            and (not original_data_sources)
        ):
            _DISPATCH[child_node.structure_family](child_node, node)


_DISPATCH = {
    StructureFamily.array: _copy_array,
    StructureFamily.awkward: _copy_awkward,
    StructureFamily.container: _copy_container,
    StructureFamily.sparse: _copy_sparse,
    StructureFamily.table: _copy_table,
}
