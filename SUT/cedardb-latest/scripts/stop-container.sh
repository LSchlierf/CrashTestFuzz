CONTAINER_ID=$1

docker exec -t lazycedardb-latest-$CONTAINER_ID sh /stop-all.sh
docker stop lazycedardb-latest-$CONTAINER_ID
docker rm lazycedardb-latest-$CONTAINER_ID
