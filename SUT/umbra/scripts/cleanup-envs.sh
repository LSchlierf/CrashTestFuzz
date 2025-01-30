docker ps -aq --filter name=lazyumbra --filter status=running | xargs docker stop
docker ps -aq --filter name=lazyumbra | xargs docker rm

rm -fr ../container/container-*
