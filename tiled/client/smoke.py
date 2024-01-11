def read(node, faulty_docs=[], verbose=False):
    if node.structure_family == "container":
        for key, child_node in node.items():
            fault_result = read(child_node)
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

    return faulty_docs
