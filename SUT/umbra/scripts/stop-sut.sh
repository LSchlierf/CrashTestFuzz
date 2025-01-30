CONTAINER_ID=$1

docker exec -t lazyumbra-$CONTAINER_ID sh /stop-umbra.sh
