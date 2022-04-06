"""
This module was created to save structure methods that can be handled by
the server and the client.

"""

import enum


class StructureFamily(str, enum.Enum):
    node = "node"
    array = "array"
    dataframe = "dataframe"
    xarray_data_array = "xarray_data_array"
    xarray_dataset = "xarray_dataset"
