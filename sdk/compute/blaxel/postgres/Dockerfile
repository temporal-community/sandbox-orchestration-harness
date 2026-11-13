# Reference Blaxel sandbox template that runs Postgres, for the database-fork
# example. Push it once per workspace from this directory, then name it as the
# provider's image:
#
#   bl push
#   export SANDBOX_PROVIDER_CONFIG='{"image":"temporal-postgres"}'
#
# A snapshot carries the running database with the files, so each fork starts
# with the rows its origin had.
FROM postgres:17-alpine

# Every Blaxel sandbox image needs the sandbox API, which runs the commands.
COPY --from=ghcr.io/blaxel-ai/sandbox:latest /sandbox-api /usr/local/bin/sandbox-api

# The standard libpq variables are all the example's psql commands rely on.
ENV PGDATA=/pgdata PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGDATABASE=postgres

# Initialize the database at build time, outside the base image's
# /var/lib/postgresql/data volume, which the image would not keep.
RUN mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA" && chmod 700 "$PGDATA" \
 && su postgres -s /bin/sh -c "initdb -D $PGDATA --auth=trust"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
