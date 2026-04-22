CONTAINER_ID=$1

docker exec -t lazycedardb-local-$CONTAINER_ID sh /stop-all.sh
docker stop lazycedardb-local-$CONTAINER_ID
docker rm lazycedardb-local-$CONTAINER_ID
