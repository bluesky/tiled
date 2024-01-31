# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.

import numpy

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context
from tiled.server.app import build_app


class TimeSuite:
    def setup(self):
        tree = MapAdapter({"x": ArrayAdapter.from_array(numpy.ones((100, 100)))})
        app = build_app(tree)
        self.context = Context.from_app(app)
        self.client = from_context(self.context)

    def teardown(self):
        self.context.close()

    def time_list_tree(self):
        list(self.client)

    def time_lookup(self):
        self.client["x"]

    def time_lookup_and_read(self):
        self.client["x"].read()
