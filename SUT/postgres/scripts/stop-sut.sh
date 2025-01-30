CONTAINER_ID=$1

docker exec -t lazypostgres-$CONTAINER_ID sh /stop-postgres.sh
