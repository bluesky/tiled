# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import uvicorn

from catalog_server.client import ClientCatalog
from catalog_server.server import app
from catalog_server.example_catalogs import catalog

HOST = '0.0.0.0'
PORT = '9040'


class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        uvicorn(app, host=HOST, port=PORT)
        self.catalog = ClientCatalog.from_uri(HOST + ':' + PORT)

    def time_list():
        list(self.catalog)
