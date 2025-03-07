CONTAINER_ID=$1

docker exec -t lazyduckdb-$CONTAINER_ID sh /stop-duckdb.sh
