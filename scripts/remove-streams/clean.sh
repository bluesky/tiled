POSTGRESQL_URI=postgresql://postgres:secret@localhost:5432
CONTAINER_ID=$(docker ps --format "{{.ID}} {{.Image}}" | grep postgres | awk '{print $1}' | head -n1)
docker exec -i ${CONTAINER_ID} psql ${POSTGRESQL_URI} -U postgres < clean.sql
rm -rf /tmp/tiled-catalog-data
