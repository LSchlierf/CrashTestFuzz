CONTAINER_ID=$1

docker exec -t lazycedardb-$CONTAINER_ID sh /stop-all.sh
docker stop lazycedardb-$CONTAINER_ID
docker rm lazycedardb-$CONTAINER_ID
