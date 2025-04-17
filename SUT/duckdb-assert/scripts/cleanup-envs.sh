docker ps -aq --filter name=lazyduckdb-assert --filter status=running | xargs docker stop
docker ps -aq --filter name=lazyduckdb-assert | xargs docker rm

rm -fr ../container/container-*
