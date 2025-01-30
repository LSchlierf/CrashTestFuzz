docker ps -aq --filter name=lazyumbra-assert --filter status=running | xargs docker stop
docker ps -aq --filter name=lazyumbra-assert | xargs docker rm

rm -fr ../container/container-*

docker image rm lazyumbra-assert
