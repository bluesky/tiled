import os

import numpy as np

from locust import HttpUser, between, task


class ReadingUser(HttpUser):
    """User that reads data from Tiled using HTTP API"""

    wait_time = between(0.5, 2)

    def on_start(self):
        self.api_key = os.environ.get("TILED_SINGLE_USER_API_KEY", "secret")
        self.headers = {"Authorization": f"Apikey {self.api_key}"}
        self.discovered_paths = []
        self.discover_data()

    def discover_data(self):
        """Discover available data paths"""
        try:
            # Search for available data
            response = self.client.get("/api/v1/search/", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                if "data" in data:
                    self.discovered_paths = [
                        item["key"] for item in data["data"][:20]
                    ]  # Limit to 20 items
        except Exception:
            self.discovered_paths = []

    @task(5)
    def read_array_data(self):
        """Read array data from Tiled"""
        if not self.discovered_paths:
            self.discover_data()
            if not self.discovered_paths:
                # Fallback to root endpoint
                self.client.get("/", headers=self.headers)
                return

        path = np.random.choice(self.discovered_paths)

        # Read array data
        response = self.client.get(f"/api/v1/array/full/{path}", headers=self.headers)
        if response.status_code != 200:
            # Try reading as table if array fails
            self.client.get(
                f"/api/v1/table/full/{path}?format=text/csv", headers=self.headers
            )

    @task(3)
    def read_metadata(self):
        """Read metadata from available data"""
        if not self.discovered_paths:
            self.discover_data()
            if not self.discovered_paths:
                self.client.get("/api/v1/metadata/", headers=self.headers)
                return

        path = np.random.choice(self.discovered_paths)
        self.client.get(f"/api/v1/metadata/{path}", headers=self.headers)

    @task(5)
    def test_root_endpoint(self):
        """Test root endpoint performance"""
        self.client.get("/", headers=self.headers)

    @task(4)
    def search_data(self):
        """Search for data with various parameters"""
        search_params = [
            {},
            {"select_metadata": "start.scan_id"},
            {"select_metadata": "start.uid"},
            {"select_metadata": "scan_id"},
            {"select_metadata": "sample.color"},
            {"limit": 5},
            {"offset": 0},
        ]

        params = np.random.choice(search_params)
        self.client.get("/api/v1/search/", headers=self.headers, params=params)

    @task(2)
    def test_metadata_root(self):
        """Test metadata root endpoint"""
        self.client.get("/api/v1/metadata/", headers=self.headers)

    @task(1)
    def refresh_discovery(self):
        """Periodically refresh discovered paths"""
        self.discover_data()
