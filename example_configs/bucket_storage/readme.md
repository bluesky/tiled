# Create a local bucket for testing access to BLOBS

In this example there exists:
- A `docker-compose.yml` file capable of instantiating and running a [Minio](https://min.io/) container.
- A configuration yaml file `bucket_storage.yml` which contains information tiled needs to authenticate with the bucket storage system and write / read Binary Large Objects (BLOBS) through the Zaar adapter.

## How to run this example:
1. In one terminal window, navigate to the directory where the `docker-compose.yml` and `bucket_storage.yml` are.
2. Run `docker compose up` with adequate permissions.
3. Open another terminal window in the same location and run `tiled serve config bucket_storage.yml --api-key secret`
4. You will need to create a `storage` directory in `/example_configs/bucket_storage` for the sqlite database.
5. Create an `ipython` session and run the following commands to write array data as a BLOB in a bucket:
```python
from tiled.client import from_uri
c = from_uri('http://localhost:8000', api_key='secret')
c.write_array([1,2,3])
```
6. You will be able to see the written data in the bucket if you log in to the minio container, exposed on your machine at `http://localhost:9001/login`. </br> Use testing credentials `minioadmin` for both fields.
