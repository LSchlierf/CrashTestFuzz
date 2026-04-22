docker ps -aq --filter name=lazycedardb-latest --filter status=running | xargs docker stop
docker ps -aq --filter name=lazycedardb-latest | xargs docker rm

rm -fr ../container/container-*

docker image rm lazycedardb-latest
