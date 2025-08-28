#!/bin/bash
set -e

docker run -d --rm --name tiled-test-redis -p 6379:6379 docker.io/redis:7-alpine
docker ps
