# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import asyncio
import uvicorn
import time
import subprocess

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
        self.server_process = subprocess.Popen((f"uvicorn catalog_server.server:app"
                                                f" --host {HOST} --port {PORT}").split())
        time.sleep(5)
        self.catalog = ClientCatalog.from_uri('http://' + HOST + ':' + PORT)

    def teardown(self):
        self.server_process.terminate()

    def time_list_catalog(self):
:       list(self.catalog)

    def time_read(self):
        self.catalog[-1].read()
