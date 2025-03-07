CONTAINER_ID=$1

docker exec -t lazysqlite-$CONTAINER_ID sh /stop-all.sh
docker stop lazysqlite-$CONTAINER_ID
docker rm lazysqlite-$CONTAINER_ID
