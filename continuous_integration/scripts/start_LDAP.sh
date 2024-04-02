#!/bin/bash
set -e

# Start LDAP server in docker container
docker pull bitnami/openldap:latest
pip install docker-compose
docker-compose -f continuous_integration/docker-configs/ldap-docker-compose.yml up -d
docker ps
