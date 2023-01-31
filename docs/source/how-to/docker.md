# Run Tiled using Docker

To run tiled using docker we must first obtain a docker image containing a tiled installation.
To do this run

```
docker pull ghcr.io/bluesky/tiled:main
```

In this example we will use the docker image to serve a directory of files as explained [here](../../tutorials/serving-files.md).

First generate a directory of example files using a utility provided by Tiled.

```
python -m tiled.examples.generate_files example_files/
```

The docker container runs tiled via [gunicorn](https://gunicorn.org/) which provides horizontal scaling over workers.
Because of this we cannot configure tiled using commandline arguments and must specify a config file.

Take the following server configuration below:
```yaml
# config.yml
trees:
  - path: /
    tree: tiled.adapters.files:DirectoryAdapter.from_directory
    args:
      directory: "example_files"
authentication:
  allow_anonymous_access: true
  single_user_api_key: SECRET
```
and serve it using the docker container
```
docker run --rm -p 8000:8000 \
  --mount type=bind,source="$(pwd)",target=/deploy \
  --env TILED_CONFIG=/deploy/config.yml \
  ghcr.io/bluesky/tiled:main
```
Note that we make the data and the configuration file available to the
container via bind mounds and point tiled to the configuration file using the
`TILED_CONFIG` environment variable.
We must supply the `single_user_api_key` in the configuration so that all
workers use the same key.

This invocation can be simplified by writing a `docker-compose.yml` file.

```yaml
# docker-compose.yml

version: "3.2" # higher config versions may also work; lower will not
services:
  tiled-server:
    image: ghcr.io/bluesky/tiled:main
    volumes:
      - type: bind
        source: .
        target: /deploy
    environment:
      - TILED_CONFIG=/deploy/config.yml
    ports:
      - 8000:8000
```

With this file the tiled server can be brought up by simply running `docker-compose up`.
