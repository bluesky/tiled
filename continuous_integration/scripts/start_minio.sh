#!/bin/bash
set -e

# Start MinIO server in docker container
docker pull coollabsio/minio:latest
docker compose -f continuous_integration/docker-configs/minio-docker-compose.yml up -d
docker ps
