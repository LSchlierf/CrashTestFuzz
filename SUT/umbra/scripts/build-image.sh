HOST_UID=$(id -u "${USER}")
HOST_GID=$(id -g "${USER}")
# WAL_SYNC_METHOD = none | open_dsync | open_sync | fdatasync | fsync
WAL_SYNC_METHOD=$1
WAL_SYNC_METHOD=${WAL_SYNC_METHOD:="none"}

docker build -t lazyumbra \
    --build-arg HOST_GID=$HOST_GID \
    --build-arg HOST_UID=$HOST_UID \
    --build-arg WAL_SYNC_METHOD="$WAL_SYNC_METHOD" \
    ../docker