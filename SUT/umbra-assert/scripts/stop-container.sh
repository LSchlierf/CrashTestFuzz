CONTAINER_ID=$1

docker exec -t lazyumbra-assert-$CONTAINER_ID sh /stop-all.sh
docker stop lazyumbra-assert-$CONTAINER_ID
docker rm lazyumbra-assert-$CONTAINER_ID
