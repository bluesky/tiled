FROM docker.io/postgres
# Uses the initialization scripts (https://hub.docker.com/_/postgres/)
# Creates Database automatically from SQL file
COPY postgres-ci-db.sql /docker-entrypoint-initdb.d/init-user-db.sql
