"""
This module was created to save structure methods that can be handled by
the server and the client.

"""

import enum


class StructureFamily(str, enum.Enum):
    node = "node"
    array = "array"
    dataframe = "dataframe"
    sparse = "sparse"
