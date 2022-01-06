"""
This contains a simple tree for demonstrating access control.
See the configuration:

example_configs/toy_authentication.yml
"""
import numpy

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter

# Make a MapAdapter with a couple arrays in it.
tree = MapAdapter(
    {
        "A": ArrayAdapter.from_array(10 * numpy.ones((10, 10))),
        "B": ArrayAdapter.from_array(20 * numpy.ones((10, 10))),
        "C": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
        "D": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
    },
)
