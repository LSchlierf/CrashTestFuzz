CONTAINER_ID=$1

docker exec -t lazyduckdb-assert-$CONTAINER_ID sh /stop-duckdb-assert.sh
