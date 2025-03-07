CONTAINER_ID=$1

docker exec -t lazycedardb-$CONTAINER_ID sh /stop-cedardb.sh
