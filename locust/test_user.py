from locust import HttpUser, task, between
import os
import json
import uuid
import numpy as np
import io

class WritingUser(HttpUser):
    """User that writes data to Tiled using HTTP API"""
    wait_time = between(1, 3)
    
    def on_start(self):
        self.api_key = os.environ.get('TILED_SINGLE_USER_API_KEY', 'secret')
        self.headers = {'Authorization': f'Apikey {self.api_key}'}
        self.written_paths = []  # Keep track of what we've written
    
    @task(3)
    def write_array_data(self):
        """Write array data to Tiled"""
        # Generate simple test data
        path = f"test_array_{uuid.uuid4().hex[:8]}"
        array_data = np.random.rand(10, 5).astype(np.float32)
        metadata = {"scan_id": np.random.randint(1, 1000), "method": "test"}
        
        # First create the node with metadata
        response = self.client.post(
            f"/api/v1/metadata/{path}",
            headers=self.headers,
            json={"metadata": metadata, "specs": ["ArrayAdapter"]}
        )
        
        if response.status_code == 201:
            # Then write the array data
            buffer = io.BytesIO()
            np.save(buffer, array_data)
            buffer.seek(0)
            
            response = self.client.put(
                f"/api/v1/array/full/{path}",
                headers={**self.headers, 'Content-Type': 'application/octet-stream'},
                data=buffer.getvalue()
            )
            
            if response.status_code == 200:
                self.written_paths.append(path)
    
    @task(2)
    def write_csv_data(self):
        """Write CSV data to Tiled"""
        path = f"test_table_{uuid.uuid4().hex[:8]}"
        
        # Generate simple CSV data
        csv_data = "col_0,col_1,col_2\n"
        for i in range(20):
            row = ",".join([str(np.random.rand()) for _ in range(3)])
            csv_data += f"{row}\n"
        
        metadata = {"experiment": "load_test", "sample": {"color": "blue"}}
        
        # Create node with metadata
        response = self.client.post(
            f"/api/v1/metadata/{path}",
            headers=self.headers,
            json={"metadata": metadata, "specs": ["TableAdapter"]}
        )
        
        if response.status_code == 201:
            # Write CSV data
            response = self.client.put(
                f"/api/v1/table/full/{path}",
                headers={**self.headers, 'Content-Type': 'text/csv'},
                data=csv_data
            )
            
            if response.status_code == 200:
                self.written_paths.append(path)
    
    @task(1)
    def check_written_data(self):
        """Verify some of our written data"""
        if self.written_paths:
            path = np.random.choice(self.written_paths)
            self.client.get(f"/api/v1/metadata/{path}", headers=self.headers)

class ReadingUser(HttpUser):
    """User that reads data from Tiled using HTTP API"""
    wait_time = between(0.5, 2)
    
    def on_start(self):
        self.api_key = os.environ.get('TILED_SINGLE_USER_API_KEY', 'secret')
        self.headers = {'Authorization': f'Apikey {self.api_key}'}
        self.discovered_paths = []
        self.discover_data()
    
    def discover_data(self):
        """Discover available data paths"""
        try:
            # Search for available data
            response = self.client.get("/api/v1/search/", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    self.discovered_paths = [item['key'] for item in data['data'][:20]]  # Limit to 20 items
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
            self.client.get(f"/api/v1/table/full/{path}?format=text/csv", headers=self.headers)
    
    @task(3)
    def read_metadata(self):
        """Read metadata from available data"""
        if not self.discovered_paths:
            self.discover_data()
            if not self.discovered_paths:
                self.client.get("/api/v1/metadata", headers=self.headers)
                return
        
        path = np.random.choice(self.discovered_paths)
        self.client.get(f"/api/v1/metadata/{path}", headers=self.headers)
    
    @task(2)
    def search_data(self):
        """Search for data with various parameters"""
        search_params = [
            {},
            {"select_metadata": "scan_id"},
            {"select_metadata": "sample.color"},
        ]
        
        params = np.random.choice(search_params)
        self.client.get("/api/v1/search/", headers=self.headers, params=params)
    
    @task(1)
    def refresh_discovery(self):
        """Periodically refresh discovered paths"""
        self.discover_data()

