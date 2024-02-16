#!/usr/bin/env bash

set -e

# Start Postgres

echo "Starting Postgres"
sudo -u postgres /usr/lib/postgresql/15/bin/postgres -c config_file=/etc/postgresql/15/main/postgresql.conf \
    > /var/log/postgres.log 2>&1 &

# Wait for postgres to become available

echo "Waiting for Postgres to become available"
until /usr/lib/postgresql/15/bin/pg_isready -q -h localhost -p 5432 -U postgres; do
    sleep 0.2
    echo "."
done

echo "Postgres is ready"

# Run the migrations

echo "Running migrations"

sudo -u postgres /usr/lib/postgresql/15/bin/createdb arroyo
sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;"
/app/arroyo-bin migrate

# start the services

echo "Starting Arroyo"
exec /usr/bin/supervisord -n -c /supervisord.conf