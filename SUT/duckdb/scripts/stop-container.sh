CONTAINER_ID=$1

docker exec -t lazyduckdb-$CONTAINER_ID sh /stop-all.sh
docker stop lazyduckdb-$CONTAINER_ID
docker rm lazyduckdb-$CONTAINER_ID
