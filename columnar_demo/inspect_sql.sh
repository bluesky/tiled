#!/usr/bin/env bash
# Inspect the columnar storage database (tiled_storage) directly.
#
#   ./inspect_sql.sh               list every SQL table and its column types
#   ./inspect_sql.sh <table_name>  show that table's columns and first rows
#
# Uses `docker compose`; set COMPOSE="podman compose" to use podman.
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE=${COMPOSE:-docker compose}
psql() { $COMPOSE exec -T postgres psql -U tiled -d tiled_storage "$@"; }

if [[ $# -eq 0 ]]; then
    psql -c "
      SELECT c.table_name,
             string_agg(c.column_name || ' ' || c.data_type
                        || CASE WHEN c.data_type = 'ARRAY'
                                THEN ' (' || c.udt_name || ')' ELSE '' END,
                        E'\n' ORDER BY c.ordinal_position) AS columns,
             (xpath('/row/n/text()',
                    query_to_xml(format('SELECT count(*) AS n FROM %I', c.table_name),
                                 false, true, '')))[1]::text::int AS n_rows
      FROM information_schema.columns c
      WHERE c.table_schema = 'public' AND c.table_name LIKE 'table\_%'
      GROUP BY c.table_name ORDER BY c.table_name;"
else
    psql -c "\\d \"$1\"" \
         -c "SELECT _dataset_id, count(*) AS n_rows FROM \"$1\" GROUP BY 1 ORDER BY 1;" \
         -c "SELECT * FROM \"$1\" ORDER BY _dataset_id LIMIT 12;"
fi
