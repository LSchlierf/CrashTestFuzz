docker ps -aq --filter name=lazyduckdb --filter status=running | xargs docker stop
docker ps -aq --filter name=lazyduckdb | xargs docker rm

rm -fr ../container/container-*
