docker ps -aq --filter name=lazysqlite --filter status=running | xargs docker stop
docker ps -aq --filter name=lazysqlite | xargs docker rm

rm -fr ../container/container-*

docker image rm lazysqlite
