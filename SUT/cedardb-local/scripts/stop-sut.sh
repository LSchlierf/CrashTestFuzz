CONTAINER_ID=$1

docker exec -t lazycedardb-local-$CONTAINER_ID sh /stop-cedardb.sh
