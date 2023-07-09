#!/bin/bash
set -e

docker run -d --rm --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres
docker ps
