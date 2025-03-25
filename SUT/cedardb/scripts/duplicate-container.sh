TEMPLATE_ID=$1
NEW_ID=$2

mkdir -p ../container/container-$NEW_ID

mkfifo ../container/container-$NEW_ID/faults.fifo
touch ../container/container-$NEW_ID/lazyfs.log
touch ../container/container-$NEW_ID/cedardb.log

cp -r ../container/container-$TEMPLATE_ID/persisted ../container/container-$NEW_ID/persisted
