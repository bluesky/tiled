import os
import uuid

import numpy as np

from locust import HttpUser, between, task


class WritingUser(HttpUser):
    """User that attempts to write data to Tiled using HTTP API"""

    wait_time = between(1, 3)

    def on_start(self):
        self.api_key = os.environ.get("TILED_SINGLE_USER_API_KEY", "secret")
        self.headers = {"Authorization": f"Apikey {self.api_key}"}

    @task
    def attempt_write_array(self):
        """Attempt to write array data using correct Tiled API endpoints"""
        key = f"test_array_{uuid.uuid4().hex[:8]}"

        # Generate simple test array
        array_data = np.random.rand(5, 3).astype(np.float32)

        # Create the array structure as Tiled expects
        array_structure = {
            "shape": list(array_data.shape),
            "dtype": str(array_data.dtype),
            "chunks": [list(array_data.shape)],  # Single chunk
        }

        # Create node using /register/ endpoint (correct endpoint from client code)
        payload = {
            "key": key,
            "structure_family": "array",
            "data_sources": [
                {
                    "structure": array_structure,
                    "mimetype": "application/octet-stream",
                }
            ],
            "metadata": {"scan_id": np.random.randint(1, 1000), "method": "load_test"},
            "specs": [],
        }

        response = self.client.post(
            "/api/v1/register/",
            headers=self.headers,
            json=payload,
        )

        # If node creation succeeds, write the actual array data
        if response.status_code in [200, 201]:
            response_data = response.json()

            # Look for the "full" link in the response to write data
            if "links" in response_data and "full" in response_data["links"]:
                full_url = response_data["links"]["full"]

                # Write array data as bytes (as the client does)
                self.client.put(
                    full_url,
                    headers={
                        **self.headers,
                        "Content-Type": "application/octet-stream",
                    },
                    data=array_data.tobytes(),
                )
