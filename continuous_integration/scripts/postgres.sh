#!/bin/bash
set -e

apk add --no-cache curl

# Try and get the latest postgres test data from the tiled-example-database repository
curl -fsSLO https://github.com/bluesky/tiled-example-database/releases/latest/download/tiled_test_db_pg.sql
