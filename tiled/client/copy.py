from ..structures.core import StructureFamily
from .container import Container


def _walk_readables(node, nodes=None):
    "Walk nodes in depth first order, yielding readable nodes."
    if nodes is None:
        for child in node:
            yield from _walk_readables(node, [child])
    else:
        value = node[nodes[-1]]
        if value.structure_family == StructureFamily.container:
            value = value.base
            yield nodes
            for k, v in value.items():
                yield from _walk_readables(value, nodes + [k])
        else:
            yield nodes


def deepcopy(src, dst):
    "Copy metadata and underlying data from src to dst."
    src = src.new_variation(structure_clients="dask")
    for path in _walk_readables(src):
        # Use Container.__getitem__ explicitly in two places here to
        # work around an oddity (bug?) in bluesky-tiled-plugins.
        s = Container.__getitem__(src, tuple(path))
        copy_func = _registry[s.structure_family]
        copy_func(s, Container.__getitem__(dst, tuple(path[:-1])), path[-1])


_registry = {}


def _register(structure_family: StructureFamily):
    def f(func):
        _registry[structure_family] = func
        return func

    return f


@_register(StructureFamily.array)
def copy_array(src, dst, key):
    dst.write_array(
        src.read(),
        key=key,
        metadata=dict(src.metadata),
        specs=src.specs,
    )


@_register(StructureFamily.container)
def copy_container(src, dst, key):
    dst.create_container(
        key=key,
        metadata=dict(src.metadata),
        specs=src.specs,
    )


@_register(StructureFamily.table)
def copy_table(src, dst, key):
    dst.write_table(
        src.read(),
        key=key,
        metadata=dict(src.metadata),
        specs=src.specs,
    )
