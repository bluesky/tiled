# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.

import tempfile

import numpy
import pandas

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import DataSource
from tiled.structures.table import TableStructure


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


class CatalogSuite:
    def setup(self):
        self.directory = tempfile.TemporaryDirectory()
        self.df = pandas.DataFrame([])

        catalog = from_uri(
            f"sqlite:///{self.directory.name}/catalog.db",
            init_if_not_exists=True,
            writable_storage=self.directory.name,
        )
        self.context = Context.from_app(build_app(catalog))
        self.client = from_context(self.context)

    def teardown(self):
        self.context.close()
        self.directory.cleanup()

    def time_repeated_write(self):
        for _ in range(100):
            self.client.new(
                structure_family=StructureFamily.table,
                data_sources=[
                    DataSource(
                        structure_family=StructureFamily.table,
                        structure=TableStructure.from_pandas(self.df),
                        mimetype="text/csv",
                    ),  # or PARQUET_MIMETYPE
                ],
            )
