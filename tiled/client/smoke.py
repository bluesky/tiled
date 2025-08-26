import sys

from ..structures.core import StructureFamily


def read(node, verbose=False, strict=False):
    """

    Parameters
    ----------
    node : tiled node
        A Client node with access to the tiled server.
        It can take form of anything compatible with the values in StructureFamily.
    verbose : bool, optional
        Enables status messages while process runs. The default is False.
    strict : bool, optional
        Enables debugging mode if a faulty node is found.

    Returns
    -------
    faulty_docs : List
        A list with URIs of all the faulty nodes that were found.

    """

    faulty_entries = []
    if node.structure_family == StructureFamily.container:
        for key, child_node in node.items():
            fault_result = read(child_node, verbose=verbose, strict=strict)
            faulty_entries.extend(fault_result)
    else:
        try:
            tmp = node.read()  # noqa: F841
        except Exception as err:
            faulty_entries.append(node.uri)
            if verbose:
                print(f"ERROR: {node.item['id']} - {err!r}", file=sys.stderr)
            if strict:
                raise
        else:
            if verbose:
                print(f"SUCCESS: {node.item['id']} ", file=sys.stderr)

    return faulty_entries
