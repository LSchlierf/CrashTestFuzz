CONTAINER_ID=$1

docker exec -t lazyduckdb-assert-$CONTAINER_ID sh /stop-all.sh
docker stop lazyduckdb-assert-$CONTAINER_ID
docker rm lazyduckdb-assert-$CONTAINER_ID
