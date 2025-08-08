import json
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
    if environment.host is None:
        raise ValueError(
            "Host must be specified with --host argument, or through the web-ui."
        )

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

    @task(1)
    def read_table_data(self):
        """Read table data from our known dataset"""
        # Read the table data we created
        self.client.get(
            f"/api/v1/table/full/locust_testing/{self.environment.known_dataset_key}",
            headers=self.headers,
        )

    @task(1)
    def read_metadata(self):
        """Read metadata from our known dataset"""

        self.client.get(
            f"/api/v1/metadata/locust_testing/{self.environment.known_dataset_key}",
            headers=self.headers,
        )

    @task(1)
    def root_endpoint(self):
        """Test root endpoint performance"""
        self.client.get("/", headers=self.headers)

    @task(1)
    def metadata_root(self):
        """Test metadata root endpoint"""
        self.client.get("/api/v1/metadata/", headers=self.headers)

    @task(1)
    def read_table_partition(self):
        """Read specific partition from our known dataset"""
        self.client.get(
            f"/api/v1/table/partition/locust_testing/{self.environment.known_dataset_key}?partition=0",
            headers=self.headers,
        )

    @task(1)
    def healthz_endpoint(self):
        """Test health check endpoint"""
        self.client.get("/healthz")

    @task(1)
    def metrics_endpoint(self):
        """Test Prometheus metrics endpoint"""
        self.client.get("/api/v1/metrics", headers=self.headers)

    @task(1)
    def about_endpoint(self):
        """Test API information endpoint"""
        self.client.get("/api/v1/", headers=self.headers)

    # @task(1)
    # def distinct_endpoint(self):
    #     """Test distinct values endpoint"""
    #     self.client.get(
    #         f"/api/v1/distinct/locust_testing/{self.environment.known_dataset_key}",
    #         headers=self.headers,
    #     )

    @task(1)
    def search_root(self):
        """Test search at root level"""
        self.client.get("/api/v1/search/", headers=self.headers)

    @task(1)
    def container_full_endpoint(self):
        """Test container full data endpoint"""
        self.client.get("/api/v1/container/full/locust_testing", headers=self.headers)

    @task(1)
    def whoami_endpoint(self):
        """Test user identity endpoint"""
        self.client.get("/api/v1/auth/whoami", headers=self.headers)

    @task(1)
    def search_fulltext(self):
        """Test fulltext search queries"""
        params = {"filter[fulltext][condition][text]": "test"}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_limit(self):
        """Test search with limit parameter"""
        params = {"page[limit]": 5}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_pagination(self):
        """Test search with offset and limit parameters"""
        params = {"page[offset]": 10, "page[limit]": 5}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_sort(self):
        """Test search with sort parameter"""
        params = {"sort": "key"}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_max_depth(self):
        """Test search with max_depth parameter"""
        params = {"max_depth": 1}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_structure_family(self):
        """Test structure family search queries"""
        params = {"filter[structure_family][condition][value]": ["table"]}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_omit_links(self):
        """Test search with omit_links parameter"""
        params = {"omit_links": "true"}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_with_data_sources(self):
        """Test search with include_data_sources parameter"""
        params = {"include_data_sources": "true"}
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_eq(self):
        """Test equality search queries"""
        params = {
            "filter[eq][condition][key]": "structure_family",
            "filter[eq][condition][value]": json.dumps("table"),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_noteq(self):
        """Test not equal search queries"""
        params = {
            "filter[noteq][condition][key]": "structure_family",
            "filter[noteq][condition][value]": json.dumps("array"),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_comparison(self):
        """Test comparison search queries"""
        params = {
            "filter[comparison][condition][operator]": "gt",
            "filter[comparison][condition][key]": "id",
            "filter[comparison][condition][value]": json.dumps(0),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_regex(self):
        """Test regex search queries"""
        params = {
            "filter[regex][condition][key]": "key",
            "filter[regex][condition][pattern]": ".*",
            "filter[regex][condition][case_sensitive]": json.dumps(True),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_like(self):
        """Test like search queries"""
        params = {
            "filter[like][condition][key]": "key",
            "filter[like][condition][pattern]": json.dumps("%"),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_contains(self):
        """Test contains search queries"""
        params = {
            "filter[contains][condition][key]": "key",
            "filter[contains][condition][value]": json.dumps("locust"),
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_in(self):
        """Test in search queries"""
        params = {
            "filter[in][condition][key]": "structure_family",
            "filter[in][condition][value]": '["table", "array"]',
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(1)
    def search_notin(self):
        """Test not in search queries"""
        params = {
            "filter[notin][condition][key]": "structure_family",
            "filter[notin][condition][value]": '["sparse"]',
        }
        self.client.get("/api/v1/search/", headers=self.headers, params=params)
