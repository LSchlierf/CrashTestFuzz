CONTAINER_ID=$1

docker exec -t lazysqlite-$CONTAINER_ID sh /stop-sqlite.sh
