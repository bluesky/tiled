# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import time
import subprocess

from catalog_server.client import ClientCatalog

HOST = "0.0.0.0"
PORT = 9040


class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """

    def setup(self):
        self.server_process = subprocess.Popen(
            (
                f"uvicorn catalog_server.server:app" f" --host {HOST} --port {PORT}"
            ).split()
        )
        time.sleep(5)
        self.catalog = ClientCatalog.from_uri(f"http://{HOST}:{PORT}")

    def teardown(self):
        self.server_process.terminate()
        self.server_process.wait()

    def time_list_catalog(self):
        list(self.catalog)

    def time_metadata(self):
        self.catalog["medium"]["ones"].metadata

    def time_describe(self):
        self.catalog["medium"]["ones"].describe()

    def time_read(self):
        self.catalog["medium"]["ones"].read()

    def time_compute(self):
        self.catalog["medium"]["ones"].read().compute()
