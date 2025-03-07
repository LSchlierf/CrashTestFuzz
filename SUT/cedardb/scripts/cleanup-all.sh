docker ps -aq --filter name=lazycedardb --filter status=running | xargs docker stop
docker ps -aq --filter name=lazycedardb | xargs docker rm

rm -fr ../container/container-*

docker image rm lazycedardb
