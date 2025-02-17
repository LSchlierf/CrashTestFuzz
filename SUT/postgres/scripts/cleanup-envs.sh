docker ps -aq --filter name=lazypostgres --filter status=running | xargs docker stop
docker ps -aq --filter name=lazypostgres | xargs docker rm

rm -fr ../container/container-*
