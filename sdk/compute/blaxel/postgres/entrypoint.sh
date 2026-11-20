#!/bin/sh
# Blaxel's image conversion leaves files owned by root and drops the socket
# directory, so restore both before starting Postgres.
mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql
chown -R postgres:postgres "$PGDATA" && chmod 700 "$PGDATA"
# Start Postgres before the sandbox API, so the sandbox is ready only once the
# database is. The Alpine image has no su-exec.
su postgres -s /bin/sh -c "pg_ctl -D $PGDATA -w -l /tmp/postgres.log -o '-c listen_addresses=127.0.0.1' start"
exec /usr/local/bin/sandbox-api
