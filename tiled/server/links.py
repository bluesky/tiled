"""
Generate the 'links' section of the response JSON.

The links vary by structure family.
"""
from ..structures.core import StructureFamily


def links_for_node(structure_family, structure, base_url, path_str, **kwargs):
    links = {}
    links = LINKS_BY_STRUCTURE_FAMILY[structure_family](
        structure_family, structure, base_url, path_str, **kwargs
    )
    links["self"] = f"{base_url}/metadata/{path_str}"
    return links


def links_for_array(
    structure_family, structure, base_url, path_str, part=None, is_composite_part=False
):
    links = {}
    namespace_shifter = "/composite" if is_composite_part else ""
    block_template = ",".join(f"{{{index}}}" for index in range(len(structure.shape)))
    links[
        "block"
    ] = f"{base_url}{namespace_shifter}/array/block/{path_str}?block={block_template}"
    links["full"] = f"{base_url}{namespace_shifter}/array/full/{path_str}"
    if part:
        links["block"] += f"&part={part}"
        links["full"] += f"?part={part}"
    return links


def links_for_awkward(
    structure_family, structure, base_url, path_str, part=None, is_composite_part=False
):
    links = {}
    namespace_shifter = "/composite" if is_composite_part else ""
    links["buffers"] = f"{base_url}{namespace_shifter}/awkward/buffers/{path_str}"
    links["full"] = f"{base_url}{namespace_shifter}/awkward/full/{path_str}"
    if part:
        links["buffers"] += f"?part={part}"
        links["full"] += f"?part={part}"
    return links


def links_for_container(structure_family, structure, base_url, path_str, **kwargs):
    links = {}
    links["full"] = f"{base_url}/container/full/{path_str}"
    links["search"] = f"{base_url}/search/{path_str}"
    return links


def links_for_composite(structure_family, structure, base_url, path_str, **kwargs):
    links = {}
    links["full"] = f"{base_url}/container/full/{path_str}"
    return links


def links_for_table(
    structure_family, structure, base_url, path_str, part=None, is_composite_part=False
):
    links = {}
    namespace_shifter = "/composite" if is_composite_part else ""
    links[
        "partition"
    ] = f"{base_url}{namespace_shifter}/table/partition/{path_str}?partition={{index}}"
    links["full"] = f"{base_url}{namespace_shifter}/table/full/{path_str}"
    if part:
        links["partition"] += f"&part={part}"
        links["full"] += f"?part={part}"
    return links


LINKS_BY_STRUCTURE_FAMILY = {
    StructureFamily.array: links_for_array,
    StructureFamily.awkward: links_for_awkward,
    StructureFamily.composite: links_for_composite,
    StructureFamily.container: links_for_container,
    StructureFamily.sparse: links_for_array,  # sparse and array are the same
    StructureFamily.table: links_for_table,
}
