"""
Example using numpy structured dtypes

https://numpy.org/doc/stable/user/basics.rec.html

"""
import numpy
from tiled.trees.in_memory import Tree
from tiled.readers.array import StructuredArrayTabularAdapter


tree = Tree(
    {
        "A": StructuredArrayTabularAdapter.from_array(
            numpy.array(
                [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
            )
        ),
    },
)
