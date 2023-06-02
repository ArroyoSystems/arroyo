#!/usr/bin/env bash

set -e

# Start Postgres

echo "Starting Postgres"
sudo -u postgres /usr/lib/postgresql/14/bin/postgres -c config_file=/etc/postgresql/14/main/postgresql.conf \
    > /var/log/postgres.log 2>&1 &

# Wait for postgres to become available

echo "Waiting for Postgres to become available"
until /usr/lib/postgresql/14/bin/pg_isready -q -h localhost -p 5432 -U postgres; do
    sleep 0.2
    echo "."
done

echo "Postgres is ready"

# Run the migrations

echo "Running migrations"

sudo -u postgres /usr/lib/postgresql/14/bin/dropdb arroyo
sudo -u postgres /usr/lib/postgresql/14/bin/createdb arroyo
refinery migrate -c /opt/arroyo/src/refinery.toml -p /opt/arroyo/src/arroyo-api/migrations

# start the services

echo "Starting Arroyo services"

exec /usr/bin/supervisord -n -c /opt/arroyo/src/docker/single/supervisord.conf
