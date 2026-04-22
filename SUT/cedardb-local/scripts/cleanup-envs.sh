docker ps -aq --filter name=lazycedardb-local --filter status=running | xargs docker stop
docker ps -aq --filter name=lazycedardb-local | xargs docker rm

rm -fr ../container/container-*
