"""
Generate the 'links' section of the response JSON.

The links vary by structure family.
"""
from ..structures.core import StructureFamily


def links_for_node(structure_family, structure, base_url, path_str):
    links = {}
    links = LINKS_BY_STRUCTURE_FAMILY[structure_family](
        structure_family, structure, base_url, path_str
    )
    links["self"] = f"{base_url}/metadata/{path_str}"
    return links


def links_for_array(structure_family, structure, base_url, path_str):
    links = {}
    block_template = ",".join(f"{{{index}}}" for index in range(len(structure.shape)))
    links["block"] = f"{base_url}/array/block/{path_str}?block={block_template}"
    links["full"] = f"{base_url}/array/full/{path_str}"
    return links


def links_for_awkward(structure_family, structure, base_url, path_str):
    links = {}
    links["buffers"] = f"{base_url}/awkward/buffers/{path_str}"
    links["full"] = f"{base_url}/awkward/full/{path_str}"
    return links


def links_for_container(structure_family, structure, base_url, path_str):
    links = {}
    links["full"] = f"{base_url}/container/full/{path_str}"
    links["search"] = f"{base_url}/search/{path_str}"
    return links


def links_for_table(structure_family, structure, base_url, path_str):
    links = {}
    links["partition"] = f"{base_url}/table/partition/{path_str}?partition={{index}}"
    links["full"] = f"{base_url}/table/full/{path_str}"
    return links


LINKS_BY_STRUCTURE_FAMILY = {
    StructureFamily.array: links_for_array,
    StructureFamily.awkward: links_for_awkward,
    StructureFamily.container: links_for_container,
    StructureFamily.sparse: links_for_array,  # spare and array are the same
    StructureFamily.table: links_for_table,
}
