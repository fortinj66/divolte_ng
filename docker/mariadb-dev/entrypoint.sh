#!/bin/bash
# Dev-only MariaDB bootstrap: initializes the data directory and creates
# the app database/user on first run, then just execs mariadbd - a real
# deployment would use replication/backup tooling this container
# deliberately has none of.
set -euo pipefail

: "${MARIADB_ROOT_PASSWORD:?MARIADB_ROOT_PASSWORD is required}"
: "${MARIADB_DATABASE:=divolte_config}"
: "${MARIADB_USER:?MARIADB_USER is required}"
: "${MARIADB_PASSWORD:?MARIADB_PASSWORD is required}"

DATADIR=/var/lib/mysql
BOOTSTRAP_SOCK=/tmp/mariadb-bootstrap.sock

if [ ! -d "$DATADIR/mysql" ]; then
  echo "entrypoint: initializing MariaDB data directory"
  mariadb-install-db --user=mysql --datadir="$DATADIR" >/dev/null

  # Bring mariadbd up on a local socket only (no networking yet) so the
  # bootstrap SQL below runs before anything else can reach this instance.
  mariadbd --user=mysql --datadir="$DATADIR" --skip-networking --socket="$BOOTSTRAP_SOCK" &
  bootstrap_pid="$!"

  for _ in $(seq 1 60); do
    mariadb --socket="$BOOTSTRAP_SOCK" -uroot -e "SELECT 1" >/dev/null 2>&1 && break
    sleep 0.5
  done

  mariadb --socket="$BOOTSTRAP_SOCK" -uroot <<-SQL
    -- mariadb-install-db creates anonymous-user entries (User='') keyed to
    -- this host's own hostname/localhost - these outrank 'root'@'%' by host
    -- specificity and require an empty password, so any password-bearing
    -- connection that resolves to one of those hosts gets rejected before
    -- 'root'@'%' is ever considered. Drop them; nothing here relies on
    -- anonymous access.
    DELETE FROM mysql.user WHERE User = '';
    ALTER USER 'root'@'localhost' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD}';
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
    CREATE DATABASE IF NOT EXISTS \`${MARIADB_DATABASE}\`;
    CREATE USER IF NOT EXISTS '${MARIADB_USER}'@'%' IDENTIFIED BY '${MARIADB_PASSWORD}';
    GRANT ALL PRIVILEGES ON \`${MARIADB_DATABASE}\`.* TO '${MARIADB_USER}'@'%';
    FLUSH PRIVILEGES;
SQL

  mariadb-admin --socket="$BOOTSTRAP_SOCK" -uroot -p"${MARIADB_ROOT_PASSWORD}" shutdown
  wait "$bootstrap_pid" 2>/dev/null || true
  echo "entrypoint: bootstrap complete"
fi

exec mariadbd --user=mysql --datadir="$DATADIR" --bind-address=0.0.0.0
