CONTAINER_ID=$1

mkdir -p ../container/container-$CONTAINER_ID

mkfifo ../container/container-$CONTAINER_ID/faults.fifo
mkdir -p ../container/container-$CONTAINER_ID/persisted
touch ../container/container-$CONTAINER_ID/lazyfs.log
touch ../container/container-$CONTAINER_ID/sqlite.log
