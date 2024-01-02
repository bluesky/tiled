#!/bin/bash
set -e

# Try and get the latest postgres test data from the tiled-example-database repository
curl -fsSLO https://github.com/bluesky/tiled-example-database/releases/latest/download/tiled_test_db_pg.sql

docker run -d --rm --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=tiled-example-data -v ./tiled_test_db_pg.sql:/docker-entrypoint-initdb.d/tiled_test_db_pg.sql -d docker.io/postgres:16
docker ps
