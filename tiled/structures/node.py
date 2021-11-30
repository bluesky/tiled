from dataclasses import dataclass


@dataclass
class NodeMacroStructure:
    count: int


@dataclass
class NodeStructure:
    micro: NodeMacroStructure
    micro = None
