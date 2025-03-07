#!/usr/bin/env bash
set -Eeuo pipefail

# This file is heavily inspired by (and partially copied from) the Postgres Dockerfile.
# We do this to ensure compatibility and make it easier to use for people that are already used to running Postgres inside docker
# https://github.com/docker-library/postgres/blob/172544062d1031004b241e917f5f3f9dfebc0df5/17/bookworm/docker-entrypoint.sh

version_check() {
  if timeout 1 ping -c 1 download.cedardb.com > /dev/null 2>&1
  	then CURRVERSION=$(curl -s https://download.cedardb.com/Dockerfile | tac | tac | head -n 4 | tail -n 1 |  awk -F'=' '{print $NF}')
  	if test "$CURRVERSION" != "$CEDARDB_VERSION"
  		then printf "
[WARNING] CedarDB out of date: Your CedarDB version (%s) is out of date
 please upgrade to the newest version (%s) at https://cedardb.com/docs/installation

" "$CEDARDB_VERSION" "$CURRVERSION"
  		else printf "
[INFO] You are running the most recent CedarDB version (%s)

" "$CURRVERSION"
  	fi
  	else printf "
[WARNING] Could not check CedarDB version

"
  fi
}

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		printf >&2 'error: both %s and %s are set (but are exclusive)
' "$var" "$fileVar"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# Loads various settings that are used elsewhere in the script
# This should be called before any other functions
setup_env() {
	file_env 'CEDAR_PASSWORD'

	file_env 'CEDAR_USER' 'postgres'
	file_env 'CEDAR_DB' "$CEDAR_USER"
  file_env 'CEDAR_DOMAIN_SOCKET_ONLY' 'No'

	declare -g DATABASE_ALREADY_EXISTS
	: "${DATABASE_ALREADY_EXISTS:=}"
	# look if the database directory already exists
	if [ -d "$CEDARDB_DATA/database" ]; then
		DATABASE_ALREADY_EXISTS='true'
	fi
	cd "$CEDARDB_DATA"
}

# error if CEDAR_PASSWORD is empty and the user hasn't explicitly acknowledged that they have
# to connect to CedarDB from inside the container via domain socket
# assumes database is not set up, ie: [ -z "$DATABASE_ALREADY_EXISTS" ]
docker_verify_minimum_env() {
	if [ -z "$CEDAR_PASSWORD" ] && [ 'yes' != "$CEDAR_DOMAIN_SOCKET_ONLY" ]; then
		cat >&2 <<-'EOE'
			[ERROR] Database is uninitialized and superuser password is not specified.
			        You must specify CEDAR_PASSWORD to a non-empty value for the
			        superuser. For example, "-e CEDAR_PASSWORD=password" on "docker run".

        If you do not want to set a password right now, you may also explicitly
        acknowledge this via "-e CEDAR_DOMAIN_SOCKET_ONLY=yes" on "docker run".
        You then have to connect via domain socket from inside the docker container
        to set up the user.
		EOE
		exit 1
	fi
}

# Set up the database directory and instantly quit again
create_db_files() {
  printf "[INFO] Setting up database directory
"
  sql -createdb "$CEDARDB_DATA/database" /dev/null <(echo "\q") > /dev/null 2>&1
}


# Execute sql script, passed via stdin (or -f flag of pqsl)
# usage: process_sql [psql-cli-args]
#    ie: process_sql --dbname=mydb <<<'INSERT ...'
#    ie: process_sql -f my-file.sql
#    ie: process_sql <my-file.sql
process_sql() {
	local query_runner=( psql -v ON_ERROR_STOP=1 -p 5432 --no-password --no-psqlrc )
	if [ -n "$CEDAR_DB" ]; then
		query_runner+=( --dbname "$CEDAR_DB" )
	fi

	PGHOST= PGHOSTADDR= "${query_runner[@]}" "$@"
}

# create initial database and user
# uses environment variables for input: CEDAR_DB, CEDAR_USER, CEDAR_PASSWORD
setup_db() {
  local userAlreadyExists
	local dbAlreadyExists

  printf "[INFO] Creating superuser: %s
"  "$CEDAR_USER"
  userAlreadyExists="$(
    process_sql --dbname postgres -U postgres --set user="$CEDAR_USER" --tuples-only <<-'EOSQL'
			SELECT 1 FROM pg_user WHERE usename = :'user' ;
		EOSQL
  )"

  if [ -z "$userAlreadyExists" ]; then
    # User doesn't exist yet, create it
    process_sql --dbname postgres -U postgres --set user="$CEDAR_USER" --set pw="$CEDAR_PASSWORD" <<-'EOSQL'
			CREATE USER :user WITH PASSWORD :'pw' SUPERUSER;
		EOSQL
  else
    # User does exist (i.e., user chose the default user name). Change the password to the given one.
    process_sql --dbname postgres -U postgres --set user="$CEDAR_USER" --set pw="$CEDAR_PASSWORD" <<-'EOSQL'
			ALTER USER :user WITH PASSWORD :'pw' SUPERUSER;
		EOSQL
  fi
  printf '
'

  printf "[INFO] Creating database: %s
" "$CEDAR_DB"
  dbAlreadyExists="$(
    process_sql --dbname postgres -U postgres --set db="$CEDAR_DB" --tuples-only <<-'EOSQL'
			SELECT 1 FROM pg_database WHERE datname = :'db' ;
		EOSQL
  )"
  if [ -z "$dbAlreadyExists" ]; then
    process_sql --dbname postgres -U postgres --set db="$CEDAR_DB" <<-'EOSQL'
			CREATE DATABASE :"db" ;
		EOSQL
    printf '
'
  fi

  printf "[INFO] Done setting up database
"

}

# Start the database, creating SSL files if necessary
# Specify an empty address to ensure this temporary step isn't
# observable externally
start_temp_db() {
  exec server "$CEDARDB_DATA/database" -address="" "-port=$CEDARDB_PORT" "$@" > /dev/null 2>&1 &
  # Remember the pid to stop the server again later on
  CEDAR_PID="$!"
  # Wait for the server to run
  until pg_isready -U postgres > /dev/null 2>&1; do sleep 1; done
}

# Stop the database again
stop_temp_db() {
  if [ -n "$CEDAR_PID" ]; then
    kill -2 "$CEDAR_PID" # SIGINT
    # Wait for the server process to stop
    while ps -p "$CEDAR_PID" >/dev/null ; do sleep 1; done
  fi
}

# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions and permissions
process_init_files() {
	printf '
'
	local f
	for f; do
		case "$f" in
			*.sh)
				# https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
				# https://github.com/docker-library/postgres/pull/452
				if [ -x "$f" ]; then
					printf '%s: running %s
' "$0" "$f"
					"$f"
				else
					printf '%s: sourcing %s
' "$0" "$f"
					. "$f"
				fi
				;;
			*.sql)     printf '%s: running %s
' "$0" "$f"; process_sql -U "$CEDAR_USER" -f "$f"; printf '
' ;;
			*.sql.gz)  printf '%s: running %s
' "$0" "$f"; gunzip -c "$f" | process_sql -U "$CEDAR_USER"; printf '
' ;;
			*.sql.xz)  printf '%s: running %s
' "$0" "$f"; xzcat "$f" | process_sql -U "$CEDAR_USER"; printf '
' ;;
			*.sql.zst) printf '%s: running %s
' "$0" "$f"; zstd -dc "$f" | process_sql -U "$CEDAR_USER"; printf '
' ;;
			*)         printf '%s: ignoring %s
' "$0" "$f" ;;
		esac
		printf '
'
	done
}

fix_permissions() {
    # Did someone bind mount the data directory with the wrong permissions?
    if [ 999 != "$(stat -c '%u' "$CEDARDB_DATA")" ] && [ -z "$DATABASE_ALREADY_EXISTS" ]
    then
      printf "[INFO] Fixing permissions on existing directory %s
" "$CEDARDB_DATA"
      chown 999 "$CEDARDB_DATA"
    fi
}

main() {
  setup_env

  if [ "$(id -u)" = '0' ]; then
    # Called as root?
    fix_permissions

    # then step down from root and restart script as cedar user
    exec gosu cedardb "$BASH_SOURCE" "$@"
  fi


  if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
    # If the database doesn't exist, do all the init stuff

    docker_verify_minimum_env
    create_db_files

    # Required for connection via psql
    export PGPASSWORD="${PGPASSWORD:-$CEDAR_PASSWORD}"
    start_temp_db "$@"
    setup_db
    process_init_files /docker-entrypoint-initdb.d/*
    unset PGPASSWORD
    stop_temp_db

    printf '[INFO] CedarDB init process complete.
'
  else
    # Else just start the db
    printf '[INFO] CedarDB Database directory appears to contain a database; Skipping initialization.
'
  fi

  version_check

  # Finally start the database for good
  if test -f "$CEDARDB_DATA/key.pem"
     then exec server "$CEDARDB_DATA/database" -address=:: "-port=$CEDARDB_PORT" "$@"
     else exec server "$CEDARDB_DATA/database" -address=:: "-port=$CEDARDB_PORT" -createSSLFiles "$@"
  fi
}

main "$@"

