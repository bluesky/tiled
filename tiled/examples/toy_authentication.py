"""
This contains a simple tree for demonstrating access control.
See the configuration:

example_configs/toy_authentication.yml
"""
import numpy

from tiled.readers.array import ArrayAdapter
from tiled.trees.in_memory import Tree

# Make a Tree with a couple arrays in it.
tree = Tree(
    {
        "A": ArrayAdapter.from_array(10 * numpy.ones((10, 10))),
        "B": ArrayAdapter.from_array(20 * numpy.ones((10, 10))),
        "C": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
        "D": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
    },
)
