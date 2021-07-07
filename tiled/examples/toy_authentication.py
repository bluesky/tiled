"""
See example_configs/toy_authentication.yml for
server configuration that runs this example.
"""
import numpy

from tiled.utils import SpecialUsers
from tiled.readers.array import ArrayAdapter
from tiled.trees.in_memory import Tree, SimpleAccessPolicy


# Specify which entries each user is allowed to use.
# SpecialUsers.public is a sentinel that means anyone can access.
access_policy = SimpleAccessPolicy(
    {
        SpecialUsers.public: ["A"],
        "alice": ["A", "B"],
        "bob": ["A", "C"],
        "cara": SimpleAccessPolicy.ALL,
    }
)
# Make a Tree with a couple arrays in it.
tree = Tree(
    {
        "A": ArrayAdapter.from_array(10 * numpy.ones((10, 10))),
        "B": ArrayAdapter.from_array(20 * numpy.ones((10, 10))),
        "C": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
        "D": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
    },
    access_policy=access_policy,
)
