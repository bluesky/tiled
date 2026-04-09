#!/bin/bash
set -e

# Start MinIO server in docker container
docker pull minio/minio:latest
docker compose -f continuous_integration/docker-configs/minio-docker-compose.yml up -d
docker ps
