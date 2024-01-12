from ..structures.core import StructureFamily


def read(node, faulty_docs=[], verbose=False):
    """


    Parameters
    ----------
    node : tiled node
        A Client node with access to the tiled server.
        It can take form of anything compatible with the values in StructureFamily.
    faulty_docs : List, optional
        A list that keeps track of the URI of the faulty nodes
        while the method traverses through the tree recursively. The default is [].
    verbose : bool, optional
        Enables status messages while process runs. The default is False.

    Returns
    -------
    faulty_docs : List
        A list with URIs of all the faulty nodes that were found.

    """
    if node.structure_family == StructureFamily.container:
        for key, child_node in node.items():
            fault_result = read(child_node, verbose=verbose)
            if len(fault_result) > 0:
                faulty_docs.append(fault_result)
    else:
        try:
            if verbose:
                print(f"reading node with data: {node.item['id']}")
            tmp = node.read()  # noqa: F841
        except Exception:
            if verbose:
                print(
                    f"Node {node.item['id']} does not have a read method or data is fault"
                )
            faulty_docs.append(node.uri)

    return faulty_docs
