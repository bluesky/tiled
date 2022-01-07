"""
Example using numpy structured dtypes

https://numpy.org/doc/stable/user/basics.rec.html

"""
import numpy

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter

tree = MapAdapter(
    {
        "A": ArrayAdapter.from_array(
            numpy.array(
                [("Rex", 9, 81.0), ("Fido", 3, 27.0)],
                dtype=[("name", "U10"), ("age", "i4"), ("weight", "f4")],
            )
        )
    }
)
