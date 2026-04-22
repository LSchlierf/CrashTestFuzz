CONTAINER_ID=$1

docker exec -t lazycedardb-latest-$CONTAINER_ID sh /stop-cedardb.sh
