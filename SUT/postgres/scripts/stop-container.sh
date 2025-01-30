CONTAINER_ID=$1

docker exec -t lazypostgres-$CONTAINER_ID sh /stop-all.sh
docker stop lazypostgres-$CONTAINER_ID
docker rm lazypostgres-$CONTAINER_ID
