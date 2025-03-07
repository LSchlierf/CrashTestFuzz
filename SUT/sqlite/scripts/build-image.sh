HOST_UID=$(id -u "${USER}")
HOST_GID=$(id -g "${USER}")

docker build -t lazysqlite \
    --build-arg HOST_GID=$HOST_GID \
    --build-arg HOST_UID=$HOST_UID \
    ../docker