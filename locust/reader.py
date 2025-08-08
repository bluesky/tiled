import logging

import numpy as np
import pyarrow

from locust import HttpUser, between, events, task
from tiled.client import from_uri


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--api-key",
        type=str,
        default="secret",
        help="API key for Tiled authentication (default: secret)",
    )


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    environment.known_dataset_key = create_test_dataset(
        environment.host, environment.parsed_options.api_key
    )


def create_test_dataset(host, api_key):
    """Create a test dataset using Tiled client for reading tasks"""

    # Connect to Tiled server using client
    client = from_uri(host, api_key=api_key)["locust_testing"]

    rng = np.random.default_rng(seed=42)
    rng.integers(10, size=100, dtype=np.dtype("uint8"))

    # Write and read tabular data to the SQL storage
    table = pyarrow.Table.from_pydict({"a": rng.random(100), "b": rng.random(100)})

    table_client = client.create_appendable_table(table.schema)
    table_client.append_partition(table, 0)

    # Verify we can read it back
    result = table_client.read()
    logging.debug(f"Created and verified dataset: {result}")

    dataset_id = table_client.item["id"]
    client.logout()
    return dataset_id


class ReadingUser(HttpUser):
    """User that reads data from Tiled using HTTP API"""

    wait_time = between(0.5, 2)

    def on_start(self):
        self.headers = {
            "Authorization": f"Apikey {self.environment.parsed_options.api_key}"
        }

        # Create a dataset using the Tiled client for testing
        # known_dataset_key = "bbe6484d-8873-4a03-9baf-aa69df11c2f1"
        logging.debug(
            f"Created test dataset with key: {self.environment.known_dataset_key}"
        )

    @task(1)
    def read_table_data(self):
        """Read table data from our known dataset"""
        try:
            # Read the table data we created
            response = self.client.get(
                f"/api/v1/table/full/locust_testing/{self.environment.known_dataset_key}",
                headers=self.headers,
            )
            logging.debug(
                f"READ TABLE /api/v1/table/full/locust_testing/"
                f"{self.environment.known_dataset_key} - Status: {response.status_code}"
            )

            if response.status_code != 200:
                logging.error(f"Failed to read table data: {response.text}")
        except Exception:
            logging.error("EXCEPTION")

    @task(1)
    def read_metadata(self):
        """Read metadata from our known dataset"""
        if not self.environment.known_dataset_key:
            # Fallback to root metadata if no dataset was created
            self.client.get("/api/v1/metadata/", headers=self.headers)
            return

        response = self.client.get(
            f"/api/v1/metadata/locust_testing/{self.environment.known_dataset_key}",
            headers=self.headers,
        )
        logging.debug(
            f"READ METADATA /api/v1/metadata/locust_testing/"
            f"{self.environment.known_dataset_key} - Status: {response.status_code}"
        )

    @task(1)
    def test_root_endpoint(self):
        """Test root endpoint performance"""
        self.client.get("/", headers=self.headers)

    @task(1)
    def search_data(self):
        """Search for our known dataset and other data"""
        search_params = [
            {},
            {"select_metadata": "scan_id"}
            if self.environment.known_dataset_key
            else {},
            {"limit": 5},
            {"offset": 0},
        ]

        params = np.random.choice(search_params)
        response = self.client.get(
            "/api/v1/search/", headers=self.headers, params=params
        )
        logging.debug(f"SEARCH /api/v1/search/ - Status: {response.status_code}")

    @task(1)
    def test_metadata_root(self):
        """Test metadata root endpoint"""
        self.client.get("/api/v1/metadata/", headers=self.headers)

    @task(1)
    def read_table_partition(self):
        """Read specific partition from our known dataset"""
        if not self.environment.known_dataset_key:
            return

        # Read partition 0 of our table
        response = self.client.get(
            f"/api/v1/table/partition/locust_testing/{self.environment.known_dataset_key}?partition=0",
            headers=self.headers,
        )
        logging.debug(
            f"READ PARTITION /api/v1/table/partition/locust_testing/"
            f"{self.environment.known_dataset_key}?partition=0 - Status: {response.status_code}"
        )

    @task(1)
    def test_healthz_endpoint(self):
        """Test health check endpoint"""
        self.client.get("/healthz")

    @task(1)
    def test_metrics_endpoint(self):
        """Test Prometheus metrics endpoint"""
        self.client.get("/api/v1/metrics", headers=self.headers)
