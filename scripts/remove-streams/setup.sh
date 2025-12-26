POSTGRESQL_URI=postgresql://postgres:secret@localhost:5432
CONTAINER_ID=$(docker ps --format "{{.ID}} {{.Image}}" | grep postgres | awk '{print $1}' | head -n1)
docker exec -i ${CONTAINER_ID} psql ${POSTGRESQL_URI} -U postgres < setup.sql
pixi run tiled catalog init ${POSTGRESQL_URI}/catalog
mkdir /tmp/tiled-catalog-data
pixi run python populate.py
