#!/usr/bin/env bash
# env

# Setup a database
if [[ -e /var/db/umbra.db ]]; then
  echo "Using the existing database!"
else
  echo "Creating a new database!"
  umbra-sql -createdb /var/db/umbra.db <<<"ALTER ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres';" || exit 1
fi

env
# Start the server
echo "Starting the server!"
if [[ -e /var/db/umbra.cert && -e /var/db/umbra.pem ]]; then
  WAL_SYNC_METHOD=none
  exec umbra-server -address 0.0.0.0 /var/db/umbra.db || exit 1
else
  WAL_SYNC_METHOD=none 
  exec umbra-server -createSSLFiles -certFile /var/db/umbra.cert -keyFile /var/db/umbra.pem -address 0.0.0.0 /var/db/umbra.db || exit 1
fi
