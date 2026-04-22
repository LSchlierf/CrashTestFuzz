#!/usr/bin/env bash
set -Eeuo pipefail

# This file is heavily inspired by (and partially copied from) the Postgres Dockerfile.
# We do this to ensure compatibility and make it easier to use for people that are already used to running Postgres inside docker
# https://github.com/docker-library/postgres/blob/172544062d1031004b241e917f5f3f9dfebc0df5/17/bookworm/docker-entrypoint.sh

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		printf >&2 'error: both %s and %s are set (but are exclusive)\n' "$var" "$fileVar"
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

# Set up the database directory and instantly quit again
create_db_files() {
  printf "[INFO] Setting up database directory\n"
  cedardb -interactive -createdb "$CEDARDB_DATA/database" /dev/null <(echo "\q") > /dev/null 2>&1
}

# Execute a sql query with the postgres user for setting up
process_sql_setup() {
  # echo "[INFO] Running Setup SQL: $1" 1>&2
  cedardb -interactive "$CEDARDB_DATA/database" /dev/null <(echo "$1") 2>/dev/null
}

# quote a sql identifier
sql_ident() {
  local s=${1-}
  s=${s//\"/\"\"} # Duplicate "
  printf '"%s"' "$s"
}

# quote a sql string literal
sql_literal() {
  local s=${1-}
  s=${s//\'/\'\'} # Duplicate '
  printf "'%s'" "$s"
}

# create initial database and user
# uses environment variables for input: CEDAR_DB, CEDAR_USER, CEDAR_PASSWORD
setup_db() {
  # Quote the environment variables
  local userQuoted pwLiteral
  userQuoted="$(sql_ident "$CEDAR_USER")"
  pwLiteral="$(sql_literal "$CEDAR_PASSWORD")"

  local userAlreadyExists dbAlreadyExists

  printf "[INFO] Creating superuser: %s\n"  "$CEDAR_USER"
  userAlreadyExists=$(process_sql_setup "SELECT 'found' FROM pg_user WHERE usename = $(sql_literal "$CEDAR_USER")")

  if [[ "$userAlreadyExists" == *"found"* ]]; then
    # User does exist (i.e., user chose the default user name). Change the password to the given one.
    printf "[INFO] User "%s" did exist. Changing the password.\n" "$CEDAR_USER"
    process_sql_setup "ALTER USER $userQuoted WITH PASSWORD $pwLiteral;"
  else
    # User doesn't exist yet, create it
    printf "[INFO] User did not exist. Creating new user.\n"
    process_sql_setup "CREATE USER $userQuoted WITH PASSWORD $pwLiteral SUPERUSER;"
  fi

  printf "[INFO] Checking for database: %s\n" "$CEDAR_DB"
  dbAlreadyExists="$( process_sql_setup "SELECT 'found' FROM pg_database WHERE datname = $(sql_literal "$CEDAR_DB");" )"

  if [[ "$dbAlreadyExists" != *"found"* ]]; then
    printf "[INFO] Creating database: %s\n" "$CEDAR_DB"
    process_sql_setup "CREATE DATABASE $(sql_ident "$CEDAR_DB");"
  else
    printf "[INFO] Database did exist.\n"
  fi

  printf "[INFO] Done setting up database\n"
}


main() {
  setup_env 

  if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
    printf "creating db files\n"
    create_db_files
    setup_db
    printf '[INFO] CedarDB init process complete.\n'
  else
    printf 'db files present'
  fi

  local args=("$CEDARDB_DATA/database" -address=:: -port="$CEDARDB_PORT" -pgSocketDir="/var/run/postgresql")

  if [ ! -f "$CEDARDB_DATA/key.pem" ]; then
    args+=(-createSSLFiles)
  fi

  printf 'starting db'

  exec cedardb "${args[@]}" "$@"

}

main "$@"