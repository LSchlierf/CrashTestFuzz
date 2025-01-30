CONTAINER_ID=$1

docker exec -t lazyumbra-assert-$CONTAINER_ID sh /stop-umbra.sh
