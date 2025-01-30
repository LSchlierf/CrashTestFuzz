CONTAINER_ID=$1

docker exec -t lazyumbra-$CONTAINER_ID sh /stop-all.sh
docker stop lazyumbra-$CONTAINER_ID
docker rm lazyumbra-$CONTAINER_ID
