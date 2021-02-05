import numpy
from datasources import ArraySource
from in_memory_catalog import Catalog


ones = numpy.ones((3, 3))
catalog = Catalog(
    {k: ArraySource(v * ones) for k, v in zip(["ones", "twos", "threes"], [1, 2, 3])}
)
