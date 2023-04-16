#!/usr/bin/env bash

# Run the migrations
DATABASE_URL="postgres://${DATABASE_USER}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_NAME}" \
    refinery migrate -e DATABASE_URL -p /migrations

# start the services
/usr/bin/supervisord -c /supervisord.conf
