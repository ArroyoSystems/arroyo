#!/usr/bin/env bash

set -e

# Run the migrations
DATABASE_URL="postgres://${DATABASE_USER}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_NAME}" \
    refinery migrate -e DATABASE_URL -p /migrations

# start the services

# if the arg is api or controller, run the respective service, otherwise run supervisord

if [ "$1" = "api" ]; then
    ASSET_DIR=/app/dist /app/arroyo-api
elif [ "$1" = "controller" ]; then
    /app/arroyo-controller
elif [ "$1" = "" ]; then
    /usr/bin/supervisord -c /supervisord.conf
fi
