import os

from locust import HttpUser, between, task


class TiledUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        self.api_key = os.environ.get("TILED_DEV_API_KEY", "secret")
        self.headers = {"Authorization": f"Apikey {self.api_key}"}

    @task
    def get_root(self):
        """Test getting the root endpoint"""
        self.client.get("/", headers=self.headers)

    @task
    def get_metadata(self):
        """Test getting metadata endpoint"""
        self.client.get("/api/v1/metadata/", headers=self.headers)
